#![allow(unused_imports)]

use std::default;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use actix::prelude::Addr;
use actix_multipart::form::MultipartForm;
use actix_multipart::form::tempfile::TempFile;
use actix_multipart::form::text::Text;
use actix_web::{Error, http::header, HttpRequest, HttpResponse, Responder, web};
use tempfile::NamedTempFile;
use zip::write::FileOptions;
use zip::{ZipArchive, ZipWriter};

use crate::common::appdata::AppShareData;
use crate::common::model::ApiResult;
use crate::config::{__INNER_SYSTEM__TENANT, ConfigUtils, DEFAULT_TENANT, MANIFEST};
use crate::config::core::{
    ConfigActor, ConfigCmd, ConfigInfoDto, ConfigKey, ConfigResult,
};
use crate::console::model::config_model::{
    OpsConfigOptQueryListResponse, OpsConfigQueryListRequest,
};
use crate::console::{DEFAULT_NAMESPACE_INFO, NamespaceUtils};
use crate::now_millis;
use crate::raft::cluster::model::SetConfigReq;

use super::model::{NamespaceInfo, PageResult};

pub async fn query_config_list(
    request: web::Query<OpsConfigQueryListRequest>,
    config_addr: web::Data<Addr<ConfigActor>>,
) -> impl Responder {
    let mut param = request.0.to_param().unwrap();
    if let Some(tenant) = param.tenant.as_ref() {
        if tenant.as_str() == "all" {
            param.tenant = None;
        }
    }
    let cmd = ConfigCmd::QueryPageInfo(Box::from(param));
    match config_addr.send(cmd).await {
        Ok(res) => {
            let r: ConfigResult = res.unwrap();
            match r {
                ConfigResult::ConfigInfoPage(size, list) => {
                    let response = OpsConfigOptQueryListResponse {
                        count: size as u64,
                        list,
                    };
                    let v = serde_json::to_string(&response).unwrap();
                    HttpResponse::Ok()
                        .insert_header(header::ContentType(mime::APPLICATION_JSON))
                        .body(v)
                }
                _ => HttpResponse::InternalServerError().body("config result error"),
            }
        }
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

pub async fn query_history_config_page(
    request: web::Query<OpsConfigQueryListRequest>,
    config_addr: web::Data<Addr<ConfigActor>>,
) -> impl Responder {
    let param = match request.0.to_history_param() {
        Ok(param) => param,
        Err(err) => {
            return HttpResponse::InternalServerError().body(err.to_string());
        }
    };
    let cmd = ConfigCmd::QueryHistoryPageInfo(Box::new(param));
    match config_addr.send(cmd).await {
        Ok(res) => {
            let r: ConfigResult = res.unwrap();
            match r {
                ConfigResult::ConfigHistoryInfoPage(size, list) => {
                    let response = PageResult {
                        count: size as u64,
                        list,
                    };
                    let v = serde_json::to_string(&response).unwrap();
                    HttpResponse::Ok()
                        .insert_header(header::ContentType(mime::APPLICATION_JSON))
                        .body(v)
                }
                _ => HttpResponse::InternalServerError().body("config result error"),
            }
        }
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[derive(Debug, MultipartForm)]
pub struct UploadForm {
    #[multipart(rename = "tenant")]
    pub tenant: Option<Text<String>>,
    #[multipart(rename = "file")]
    pub files: Vec<TempFile>,
}

pub async fn import_config(
    req: HttpRequest,
    MultipartForm(form): MultipartForm<UploadForm>,
    app: web::Data<Arc<AppShareData>>,
) -> Result<impl Responder, Error> {
    let tenant = Arc::new(ConfigUtils::default_tenant(
        match req.headers().get("tenant") {
            Some(v) => String::from_utf8_lossy(v.as_bytes()).to_string(),
            None => "".to_owned(),
        },
    ));
    //let tenant = Arc::new(ConfigUtils::default_tenant(config_info.0.tenant.unwrap_or_default()));

    let mut folder_depth = 2;
    let mut import_by_tenant = false;
    let mut data_id_index = 1;
    let mut group_index = 0;
    let mut namespaces: Vec<NamespaceInfo> = vec![];

    for f in form.files {
        let namespace_str = get_contains_file_content(f.file.as_file(), MANIFEST);
        if !namespace_str.is_empty() {
            folder_depth = 3;
            import_by_tenant = true;
            data_id_index = 2;
            group_index = 1;
            namespaces = serde_json::from_str(&*namespace_str)?;
        }

        match zip::ZipArchive::new(f.file) {
            Ok(mut archive) => {
                for i in 0..archive.len() {
                    let mut file = archive.by_index(i).unwrap();
                    /*
                    let filepath = match file.enclosed_name() {
                        Some(path) => path,
                        None => continue,
                    };
                    */
                    let filename = file.name();
                    if !(*filename).ends_with('/') {
                        let parts = filename.split('/').collect::<Vec<_>>();

                        if parts.len() != folder_depth {
                            continue;
                        }

                        assert_eq!(parts.len(), folder_depth);

                        if MANIFEST.eq(filename) {
                            continue;
                        }


                        let mut config_key = ConfigKey::new_by_arc(
                            Arc::new(parts[data_id_index].to_owned()),
                            Arc::new(parts[group_index].to_owned()),
                            tenant.clone(),
                        );

                        if import_by_tenant {
                            config_key.tenant = if DEFAULT_TENANT.eq(&parts[0].to_owned()) {
                                Arc::from(String::new())
                            } else {
                                Arc::new(parts[0].to_owned())
                            };
                            let namespace_info = get_namespace_by_id(config_key.tenant.to_string(), namespaces.clone());
                            match NamespaceUtils::add_namespace(&app, namespace_info.clone()).await {
                                Ok(_) => log::info!("add namespace {:?} success", namespace_info.clone().namespace_name),
                                Err(e) => log::error!("add namespace {:?} error: {}", namespace_info.clone().namespace_name, e.to_string()),
                            };
                        }

                        let value = match io::read_to_string(&mut file) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        //println!("update load, {:?}:{}",&config_key,&value);
                        //config_addr.do_send(ConfigAsyncCmd::Add(config_key, Arc::new(value)));
                        let mut req = SetConfigReq::new(config_key.clone(), Arc::new(value));
                        let data_id_clone = config_key.data_id.clone();
                        req.config_type = SetConfigReq::detect_config_type(data_id_clone);

                        app.config_route.set_config(req).await.ok();
                    }
                }
            }
            Err(_) => todo!(),
        }
    }
    Ok(HttpResponse::Ok())
}

fn zip_file(mut zip: ZipWriter<&mut File>, list: Vec<ConfigInfoDto>) -> anyhow::Result<()> {
    if list.is_empty() {
        let options = FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .unix_permissions(0o755);
        zip.start_file(".ignore", options)?;
        zip.write_all("empty config".as_bytes())?;
    }
    for item in &list {
        zip.add_directory(item.group.as_str(), Default::default())
            .ok();
        let options = FileOptions::default()
            .compression_method(zip::CompressionMethod::Stored)
            .unix_permissions(0o755);
        zip.start_file(
            &format!("{}/{}", &item.group.as_str(), &item.data_id.as_str()),
            options,
        )?;
        zip.write_all(item.content.as_ref().unwrap().as_bytes())?;
    }
    zip.finish()?;
    Ok(())
}

async fn zip_file_for_tenant(mut zip: ZipWriter<&mut File>, list: Vec<ConfigInfoDto>, config_addr: web::Data<Addr<ConfigActor>>) -> anyhow::Result<()> {
    let options = FileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o755);

    if list.is_empty() {
        zip.start_file(".ignore", options)?;
        zip.write_all("empty config".as_bytes())?;
    }


    for item in &list {
        if __INNER_SYSTEM__TENANT.eq(&(*item.tenant.as_str())) {
            continue;
        }
        let tenant = if item.tenant.as_str().is_empty() {
            DEFAULT_TENANT
        } else {
            item.tenant.as_str()
        };

        zip.add_directory(&format!("{}/{}", tenant, &item.group.as_str()), Default::default())
            .ok();
        zip.start_file(
            &format!("{}/{}/{}", tenant, &item.group.as_str(), &item.data_id.as_str()),
            options,
        )?;
        zip.write_all(item.content.as_ref().unwrap().as_bytes())?;
    }

    let namespaces = NamespaceUtils::get_namespaces(&config_addr).await;
    let v = serde_json::to_string(&namespaces).unwrap();

    zip.start_file("manifest", options)?;
    zip.write_all(v.as_bytes())?;

    zip.finish()?;
    Ok(())
}

fn get_contains_file_content<R: Read + Seek>(reader: R, filename: &str) -> String {
    let mut archive = ZipArchive::new(reader).expect("Failed to read ZIP archive");

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).expect("Failed to access file in ZIP archive");
        if file.name() == filename {
            let mut content = String::new();
            file.read_to_string(&mut content).expect("Failed to read file content");
            return content;
        }
    }

    String::new()
}

fn get_namespace_by_id(namespace_id: String, namespaces: Vec<NamespaceInfo>) -> NamespaceInfo {
    if namespace_id.is_empty() {
        let mut default = NamespaceInfo::default();
        default.namespace_name = Some(DEFAULT_TENANT.to_owned());
        default.r#type = Some("0".parse().unwrap());
        return default;
    }

    for namespace in namespaces {
        if namespace.clone().namespace_id.unwrap_or_default().eq(&namespace_id) {
            return namespace.clone();
        }
    }
    NamespaceInfo::default()
}

///
/// 按查询条件导出配置
pub async fn download_config(
    request: web::Query<OpsConfigQueryListRequest>,
    config_addr: web::Data<Addr<ConfigActor>>,
) -> impl Responder {
    let mut param = request.0.to_param().unwrap();
    param.limit = 0xffff_ffff;
    param.query_context = true;
    let mut download_all = false;
    if let Some(tenant) = param.tenant.as_ref() {
        if tenant.as_str() == "all" {
            param.tenant = None;
            download_all = true
        }
    }
    let cmd = ConfigCmd::QueryPageInfo(Box::new(param));
    match config_addr.send(cmd).await {
        Ok(res) => {
            let r: ConfigResult = res.unwrap();
            match r {
                ConfigResult::ConfigInfoPage(_, list) => {
                    let mut tmpfile: File = tempfile::tempfile().unwrap();
                    {
                        let write = std::io::Write::by_ref(&mut tmpfile);
                        let zip = ZipWriter::new(write);
                        let result = if download_all {
                            zip_file_for_tenant(zip, list, config_addr).await
                        } else {
                            zip_file(zip, list)
                        };

                        result.ok();
                    }
                    // Seek to start
                    tmpfile.seek(SeekFrom::Start(0)).unwrap();
                    let mut buf = vec![];
                    tmpfile.read_to_end(&mut buf).unwrap();

                    let filename = format!("rnacos_config_export_{}.zip", now_millis());
                    HttpResponse::Ok()
                        .insert_header(header::ContentType::octet_stream())
                        .insert_header(header::ContentDisposition::attachment(filename))
                        .body(buf)
                }
                _ => HttpResponse::InternalServerError().body("config result error"),
            }
        }
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}
