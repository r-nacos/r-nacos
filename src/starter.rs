use std::{sync::Arc, collections::HashSet, time::Duration};

use crate::{common::{appdata::AppShareData, AppSysConfig}, config::core::{ConfigActor, ConfigCmd}, naming::core::{NamingActor, NamingCmd}, raft::{asyncraft::{store::{store::AStore, ClientRequest}, network::{factory::{RaftConnectionFactory, RaftClusterRequestSender}, network::RaftRouter}}, cluster::{route::{RaftAddrRouter, ConfigRoute}, model::RouterRequest}, NacosRaft}, grpc::{bistream_manage::BiStreamManage, PayloadUtils}};
use actix::prelude::*;
use async_raft::{Config, Raft, RaftStorage, raft::ClientWriteRequest};

pub fn build_share_data(sys_config:Arc<AppSysConfig>) -> anyhow::Result<Arc<AppShareData>> {
    let db = Arc::new(
        sled::Config::new()
            .path(&sys_config.config_db_dir)
            .mode(sled::Mode::LowSpace)
            .cache_capacity(10 * 1024 * 1024)
            //.flush_every_ms(Some(1000))
            .open()
            .unwrap(),
    );

    let config_addr = ConfigActor::new(db.clone()).start();
    //let naming_addr = NamingActor::new_and_create();
    let naming_addr = NamingActor::create_at_new_system();

    let store = Arc::new(AStore::new(sys_config.raft_node_id.to_owned(),db,config_addr.clone()));
    let conn_factory = RaftConnectionFactory::new(60).start();
    let cluster_sender = Arc::new(RaftClusterRequestSender::new(conn_factory));
    let raft= build_raft(&sys_config,store.clone(),cluster_sender.clone())?;
    config_addr.do_send(ConfigCmd::SetRaft(raft.clone()));

    let raft_addr_router = Arc::new(RaftAddrRouter::new(raft.clone(),store.clone(),sys_config.raft_node_id.to_owned()));
    let config_route = Arc::new(ConfigRoute::new(config_addr.clone(),raft_addr_router,cluster_sender.clone()));

    let mut bistream_manage = BiStreamManage::new();
    bistream_manage.set_config_addr(config_addr.clone());
    bistream_manage.set_naming_addr(naming_addr.clone());
    let bistream_manage_addr = bistream_manage.start();
    config_addr.do_send(ConfigCmd::SetConnManage(bistream_manage_addr.clone()));
    naming_addr.do_send(NamingCmd::SetConnManage(bistream_manage_addr.clone()));
    let bistream_manage_http_addr = bistream_manage_addr.clone();

    let app_data = Arc::new(AppShareData{
        config_addr:config_addr.clone(),
        naming_addr:naming_addr.clone(),
        bi_stream_manage: bistream_manage_http_addr.clone(),
        raft:raft.clone(),
        raft_store:store,
        sys_config: sys_config.clone(),
        config_route,
        cluster_sender,
    });
    Ok(app_data)
}

fn build_raft(sys_config: &Arc<AppSysConfig>,store:Arc<AStore>,cluster_sender:Arc<RaftClusterRequestSender>) -> anyhow::Result<Arc<NacosRaft>> {
    let config = Config::build("rnacos raft".to_owned())
        .heartbeat_interval(500) 
        .election_timeout_min(1500) 
        .election_timeout_max(3000) 
        .validate().unwrap();
    let config = Arc::new(config);
    let network = Arc::new(RaftRouter::new(store.clone(),cluster_sender.clone()));
    let raft = Arc::new(Raft::new(sys_config.raft_node_id.to_owned(),config.clone(),network,store.clone()));
    if sys_config.raft_auto_init {
        tokio::spawn(auto_init_raft(store, raft.clone(),sys_config.clone()));
    }
    else if !sys_config.raft_join_addr.is_empty() {
        tokio::spawn(auto_join_raft(store,sys_config.clone(),cluster_sender));
    }
    Ok(raft)
}

async fn auto_init_raft(store:Arc<AStore>,raft:Arc<NacosRaft>,sys_config: Arc<AppSysConfig>) -> anyhow::Result<()> {
    let state = store.get_initial_state().await?;
    if state.last_log_term==0 && state.last_log_index==0 {
        log::info!("auto init raft. node_id:{},addr:{}",&sys_config.raft_node_id,&sys_config.raft_node_addr);
        let mut members = HashSet::new();
        members.insert(sys_config.raft_node_id.to_owned());
        raft.initialize(members).await.ok();
        raft.client_write(ClientWriteRequest::new(ClientRequest::NodeAddr { id:sys_config.raft_node_id, addr: Arc::new(sys_config.raft_node_addr.to_owned())})).await.ok();
    }
    Ok(())
}

async fn auto_join_raft(store:Arc<AStore>,sys_config: Arc<AppSysConfig>,cluster_sender:Arc<RaftClusterRequestSender>) -> anyhow::Result<()> {
    let state = store.get_initial_state().await?;
    if state.last_log_term==0 && state.last_log_index==0 {
        //wait for self raft network started
        tokio::time::sleep(Duration::from_millis(500)).await;
        let req = RouterRequest::JoinNode { node_id: sys_config.raft_node_id.to_owned(), node_addr: Arc::new(sys_config.raft_node_addr.to_owned())};
        let request = serde_json::to_string(&req).unwrap_or_default();
        let payload = PayloadUtils::build_payload("RaftRouteRequest", request);
        cluster_sender.send_request(Arc::new(sys_config.raft_join_addr.to_owned()), payload).await?;
        log::info!("auto join raft,join_addr:{}.node_id:{},addr:{}",&sys_config.raft_join_addr,&sys_config.raft_node_id,&sys_config.raft_node_addr);
    }
    Ok(())
}