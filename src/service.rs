use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use tokio::runtime::Handle;
use tokio::sync::mpsc::channel;
use tracing::error;
use tracing::info;
use tracing::trace;
use windows_service::service::Service;
use windows_service::service::ServiceAccess;
use windows_service::service::ServiceControl;
use windows_service::service::ServiceControlAccept;
use windows_service::service::ServiceErrorControl;
use windows_service::service::ServiceInfo;
use windows_service::service::ServiceState;
use windows_service::service::ServiceStatus;
use windows_service::service::ServiceType;
use windows_service::service_control_handler;
use windows_service::service_control_handler::ServiceControlHandlerResult;
use windows_service::service_manager::ServiceManager;
use windows_service::service_manager::ServiceManagerAccess;
use windows_sys::Win32::Foundation::ERROR_SERVICE_DOES_NOT_EXIST;

pub const SERVICE_NAME: &str = "ARCUExtractDownloader";
const SERVICE_DISPLAY_NAME: &str = "ARCU PCCU Extract Downloader";
// const SERVICE_DESCRIPTION: &str = "Manage ARCU Extracts from Symitar";

pub async fn run_service() -> Result<()> {
    info!("Run_service");
    let (shutdown_tx, shutdown_rx) = channel(8);
    let status_handle =
        service_control_handler::register(
            SERVICE_NAME,
            move |control_event| match control_event {
                ServiceControl::Stop => {
                    Handle::current()
                        .block_on(shutdown_tx.send(()))
                        .expect("Could not send shutdown message.");
                    ServiceControlHandlerResult::NoError
                }
                _ => ServiceControlHandlerResult::NotImplemented,
            },
        )?;
    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP,
        exit_code: windows_service::service::ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(10),
        process_id: None,
    })?;

    info!("Service running");

    tokio::spawn(crate::run(
        async move || {
            trace!("Sending service Stop status");
            if let Err(e) = status_handle.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::Stopped,
                controls_accepted: ServiceControlAccept::STOP,
                exit_code: windows_service::service::ServiceExitCode::Win32(0),
                checkpoint: 0,
                wait_hint: Duration::from_secs(0),
                process_id: None,
            }) {
                error!("Service failed: {:?}", e);
            };
        },
        Some(shutdown_rx),
    ))
    .await?
}

pub fn install() -> Result<()> {
    let manager =
        ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CREATE_SERVICE)?;
    let service_info = ServiceInfo {
        name: SERVICE_NAME.into(),
        display_name: SERVICE_DISPLAY_NAME.into(),
        service_type: ServiceType::OWN_PROCESS,
        start_type: windows_service::service::ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: std::env::current_exe().expect("Couldn't get executable path"),
        launch_arguments: vec![],
        dependencies: vec![],
        account_name: None,
        account_password: None,
    };
    let service_access = ServiceAccess::START | ServiceAccess::STOP;
    manager.create_service(&service_info, service_access)?;
    info!("Service installed successfully");
    Ok(())
}

pub fn uninstall() -> Result<()> {
    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
    let service_access = ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE;
    let service = manager.open_service(SERVICE_NAME, service_access)?;
    _uninstall(service, &manager)
}

fn _uninstall(service: Service, manager: &ServiceManager) -> Result<()> {
    service.delete()?;
    if service.query_status()?.current_state != ServiceState::Stopped {
        service.stop()?;
    }
    drop(service);

    // From library example:

    // Win32 API does not give us a way to wait for service deletion.
    // To check if the service is deleted from the database, we have to poll it ourselves.
    let start = Instant::now();
    let timeout = Duration::from_secs(5);
    while start.elapsed() < timeout {
        if let Err(windows_service::Error::Winapi(e)) =
            manager.open_service(SERVICE_NAME, ServiceAccess::QUERY_STATUS)
        {
            if e.raw_os_error() == Some(ERROR_SERVICE_DOES_NOT_EXIST as i32) {
                info!("{SERVICE_NAME} is deleted.");
                return Ok(());
            }
        }
        sleep(Duration::from_secs(1));
    }

    Ok(())
}

pub fn reinstall() -> Result<()> {
    let manager = ServiceManager::local_computer(
        None::<&str>,
        ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
    )?;
    let service_access = ServiceAccess::QUERY_STATUS
        | ServiceAccess::STOP
        | ServiceAccess::DELETE
        | ServiceAccess::START;
    let service = manager.open_service(SERVICE_NAME, service_access)?;
    _uninstall(service, &manager)?;

    let service_info = ServiceInfo {
        name: SERVICE_NAME.into(),
        display_name: SERVICE_DISPLAY_NAME.into(),
        service_type: ServiceType::OWN_PROCESS,
        start_type: windows_service::service::ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: std::env::current_exe().expect("Couldn't get executable path"),
        launch_arguments: vec![],
        dependencies: vec![],
        account_name: None,
        account_password: None,
    };

    manager.create_service(&service_info, service_access)?;
    info!("Service installed successfully");

    Ok(())
}
