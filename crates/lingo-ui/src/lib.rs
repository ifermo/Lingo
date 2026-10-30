//! Slint user interface and the bridge to the core service.

slint::include_modules!();

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use flume::{Receiver, Sender};
use lingo_core::model::{
    AppState, Avatar, Device, DeviceId, ManifestEntry, Role, Staging, Transfer, TransferStatus,
    now_micros,
};
use lingo_core::msg::{
    ClientSend, Empty, Envelope, LinkAccept, RemoveDevice, SetAvatar, StagingAdd, StagingClear,
    StagingNote, TransferAccept, TransferRef, TransferRemove,
};
use slint::{CloseRequestResponse, ComponentHandle, ModelRc, Timer, TimerMode, VecModel};

pub struct UiOptions {
    pub hidden: bool,
    pub gallery: bool,
    pub background_mode: Arc<AtomicBool>,
    pub screenshot: Option<PathBuf>,
    pub page: i32,
    pub show_incoming: bool,
}

#[derive(Default)]
struct ViewState {
    app: AppState,
    devices: BTreeMap<i64, Device>,
    transfers: BTreeMap<i64, Transfer>,
    page: i32,
    selected_device: i32,
    link_code: String,
    background_mode: bool,
    notifications: bool,
}

pub fn run(
    options: UiOptions,
    commands: Option<Sender<Envelope>>,
    events: Option<Receiver<Envelope>>,
) -> Result<(), slint::PlatformError> {
    let ui = AppWindow::new()?;
    if matches!(std::env::var("LINGO_DARK").as_deref(), Ok("1")) {
        ui.global::<Theme>().set_dark(true);
    }

    let state = Rc::new(RefCell::new(ViewState {
        notifications: true,
        ..ViewState::default()
    }));

    if options.gallery {
        let mut state = state.borrow_mut();
        seed_gallery(&ui, &mut state);
        state.page = options.page;
        rebind_all(&ui, &state);
    } else if let Some(rx) = &events {
        while let Ok(envelope) = rx.try_recv() {
            apply_envelope(&ui, &mut state.borrow_mut(), envelope);
        }
    }
    rebind_all(&ui, &state.borrow());

    if options.gallery && options.show_incoming {
        ui.set_show_incoming(true);
        ui.set_incoming_id(42);
        ui.set_incoming_title("Jessica wants to send files".into());
        ui.set_incoming_subtitle("3 files · 5.2 MB · Reference".into());
    }

    wire_callbacks(
        &ui,
        state.clone(),
        commands,
        options.background_mode.clone(),
        options.gallery,
    );

    let tray = Tray::new()?;
    tray.set_glyph(tray_icon());
    {
        let weak = ui.as_weak();
        tray.on_open_requested(move || {
            if let Some(ui) = weak.upgrade() {
                let _ = ui.show();
            }
        });
    }
    tray.on_quit_requested(|| {
        let _ = slint::quit_event_loop();
    });

    {
        let weak = ui.as_weak();
        let background_mode = options.background_mode.clone();
        ui.window().on_close_requested(move || {
            if background_mode.load(Ordering::Relaxed) {
                if let Some(ui) = weak.upgrade() {
                    let _ = ui.hide();
                }
                CloseRequestResponse::KeepWindowShown
            } else {
                let _ = slint::quit_event_loop();
                CloseRequestResponse::HideWindow
            }
        });
    }

    let _timer = events.map(|rx| {
        let weak = ui.as_weak();
        let state = state.clone();
        let timer = Timer::default();
        timer.start(TimerMode::Repeated, Duration::from_millis(80), move || {
            let Some(ui) = weak.upgrade() else {
                return;
            };
            let mut state = state.borrow_mut();
            let mut changed = false;
            while let Ok(envelope) = rx.try_recv() {
                apply_envelope(&ui, &mut state, envelope);
                changed = true;
            }
            if changed {
                rebind_all(&ui, &state);
            }
        });
        timer
    });

    if options.hidden {
        ui.hide()?;
    }

    if let Some(path) = options.screenshot {
        let weak = ui.as_weak();
        Timer::single_shot(Duration::from_millis(900), move || {
            if let Some(ui) = weak.upgrade() {
                match ui.window().take_snapshot() {
                    Ok(snapshot) => {
                        let width = snapshot.width();
                        let height = snapshot.height();
                        match image::save_buffer(
                            &path,
                            snapshot.as_bytes(),
                            width,
                            height,
                            image::ExtendedColorType::Rgba8,
                        ) {
                            Ok(()) => {
                                eprintln!("screenshot saved: {} ({width}x{height})", path.display())
                            }
                            Err(err) => eprintln!("screenshot encode failed: {err}"),
                        }
                    }
                    Err(err) => eprintln!("screenshot snapshot failed: {err}"),
                }
                let _ = slint::quit_event_loop();
            }
        });
    }

    let result = ui.run();
    drop(tray);
    result
}

fn wire_callbacks(
    ui: &AppWindow,
    state: Rc<RefCell<ViewState>>,
    commands: Option<Sender<Envelope>>,
    background_mode: Arc<AtomicBool>,
    demo: bool,
) {
    let weak = ui.as_weak();

    {
        let state = state.clone();
        let weak = weak.clone();
        ui.on_nav(move |page| {
            state.borrow_mut().page = page;
            if let Some(ui) = weak.upgrade() {
                ui.set_page(page);
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_add_files(move || {
            if let Some(paths) = rfd::FileDialog::new().pick_files() {
                add_paths(&state, &commands, weak.upgrade().as_ref(), paths, demo);
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_add_folder(move || {
            if let Some(path) = rfd::FileDialog::new().pick_folder() {
                add_paths(&state, &commands, weak.upgrade().as_ref(), vec![path], demo);
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_paste_files(move || match clipboard_files::read() {
            Ok(paths) if !paths.is_empty() => {
                add_paths(&state, &commands, weak.upgrade().as_ref(), paths, demo);
            }
            _ => {
                if let Some(ui) = weak.upgrade() {
                    toast(
                        &ui,
                        "No files found",
                        "Copy some files first, then try again.",
                        "warning",
                    );
                }
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_clear_staging(move || {
            if demo {
                let mut state = state.borrow_mut();
                state.app.staging = Staging::default();
                if let Some(ui) = weak.upgrade() {
                    rebind_all(&ui, &state);
                }
            } else {
                send(&commands, Envelope::StagingClear(StagingClear::default()));
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        ui.on_note_changed(move |note| {
            state.borrow_mut().app.staging.note = note.to_string();
            if !demo {
                send(
                    &commands,
                    Envelope::StagingNote(StagingNote {
                        note: note.to_string(),
                    }),
                );
            }
        });
    }

    {
        let state = state.clone();
        let weak = weak.clone();
        ui.on_select_device(move |id| {
            state.borrow_mut().selected_device = id;
            if let Some(ui) = weak.upgrade() {
                ui.set_selected_device(id);
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_send(move || {
            let selected = state.borrow().selected_device;
            if selected < 0 {
                if let Some(ui) = weak.upgrade() {
                    toast(
                        &ui,
                        "Choose a device",
                        "Pick where the files should go.",
                        "info",
                    );
                }
                return;
            }
            if demo {
                if let Some(ui) = weak.upgrade() {
                    toast(
                        &ui,
                        "Demo mode",
                        "This build stages files locally; networking arrives next.",
                        "info",
                    );
                }
            } else {
                send(
                    &commands,
                    Envelope::ClientSend(ClientSend {
                        dst: selected as i64,
                    }),
                );
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_transfer_action(move |kind, id| {
            let kind = kind.to_string();
            if kind == "open" {
                let path = state
                    .borrow()
                    .transfers
                    .get(&(id as i64))
                    .and_then(|transfer| transfer.dir.clone());
                if let Some(path) = path {
                    open_in_finder(&path);
                }
                return;
            }
            if demo {
                let mut state = state.borrow_mut();
                if let Some(transfer) = state.transfers.get_mut(&(id as i64)) {
                    match kind.as_str() {
                        "pause" => transfer.status = TransferStatus::Paused,
                        "resume" => transfer.status = TransferStatus::Ready,
                        "cancel" => transfer.status = TransferStatus::Canceled,
                        "remove" | "delete" => transfer.removed = true,
                        _ => {}
                    }
                }
                if let Some(ui) = weak.upgrade() {
                    rebind_all(&ui, &state);
                }
                return;
            }
            let envelope = match kind.as_str() {
                "pause" => Some(Envelope::TransferPause(TransferRef { id: id as i64 })),
                "resume" => Some(Envelope::TransferResume(TransferRef { id: id as i64 })),
                "cancel" => Some(Envelope::TransferCancel(TransferRef { id: id as i64 })),
                "remove" => Some(Envelope::TransferRemove(TransferRemove {
                    id: id as i64,
                    delete_files: false,
                })),
                "delete" => Some(Envelope::TransferRemove(TransferRemove {
                    id: id as i64,
                    delete_files: true,
                })),
                _ => None,
            };
            if let Some(envelope) = envelope {
                send(&commands, envelope);
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_save_avatar(move |owner, name, variant, color| {
            let avatar = Avatar {
                owner: owner.to_string(),
                name: name.to_string(),
                variant: variant.to_string(),
                color: color.to_string(),
            };
            let setup = avatar.is_complete();
            {
                let mut state = state.borrow_mut();
                state.app.avatar = avatar.clone();
                state.app.setup = setup;
            }
            send(&commands, Envelope::SetAvatar(SetAvatar { avatar, syn: 0 }));
            if let Some(ui) = weak.upgrade() {
                ui.set_setup(setup);
                rebind_all(&ui, &state.borrow());
                toast(
                    &ui,
                    "Saved",
                    "This device is ready to send files.",
                    "success",
                );
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_became_guest(move || {
            state.borrow_mut().app.setup_account = true;
            send(&commands, Envelope::SetGuestMode(Empty {}));
            if let Some(ui) = weak.upgrade() {
                rebind_all(&ui, &state.borrow());
                toast(
                    &ui,
                    "Guest mode",
                    "You can receive files while online.",
                    "info",
                );
            }
        });
    }

    {
        let weak = weak.clone();
        ui.on_login(move || {
            if let Some(ui) = weak.upgrade() {
                toast(
                    &ui,
                    "Online sign-in",
                    "Account sign-in will be available when the sync service is connected.",
                    "info",
                );
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_logout(move || {
            {
                let mut state = state.borrow_mut();
                state.app.private_remote_state = None;
                state.app.setup_account = false;
            }
            send(&commands, Envelope::AppLogout(Empty {}));
            if let Some(ui) = weak.upgrade() {
                rebind_all(&ui, &state.borrow());
                toast(&ui, "Signed out", "You can sign back in any time.", "info");
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_add_device(move |code| {
            let code = code.trim().to_string();
            if code.is_empty() {
                if let Some(ui) = weak.upgrade() {
                    toast(
                        &ui,
                        "Add a link code",
                        "Paste the code shared by the other device.",
                        "info",
                    );
                }
                return;
            }
            send(&commands, Envelope::LinkAccept(LinkAccept { code }));
            state.borrow_mut().link_code.clear();
            if let Some(ui) = weak.upgrade() {
                ui.set_link_code("".into());
                if demo {
                    toast(
                        &ui,
                        "Link codes need the service",
                        "Pairing is enabled once the sync service is connected.",
                        "warning",
                    );
                }
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_remove_device(move |id| {
            state.borrow_mut().devices.remove(&(id as i64));
            send(
                &commands,
                Envelope::RemoveDevice(RemoveDevice { id: id as i64 }),
            );
            if let Some(ui) = weak.upgrade() {
                rebind_all(&ui, &state.borrow());
            }
        });
    }

    {
        let state = state.clone();
        let background = background_mode;
        let weak = weak.clone();
        ui.on_set_background(move |enabled| {
            background.store(enabled, Ordering::Relaxed);
            state.borrow_mut().background_mode = enabled;
            if let Some(ui) = weak.upgrade() {
                ui.set_background_mode(enabled);
                toast(
                    &ui,
                    if enabled {
                        "Staying available"
                    } else {
                        "Background mode off"
                    },
                    if enabled {
                        "Lingo keeps receiving files after the window closes."
                    } else {
                        "Closing the window will quit Lingo."
                    },
                    "info",
                );
            }
        });
    }

    {
        let state = state.clone();
        let weak = weak.clone();
        ui.on_set_notifications(move |enabled| {
            state.borrow_mut().notifications = enabled;
            if let Some(ui) = weak.upgrade() {
                ui.set_notifications(enabled);
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_accept_incoming(move |id| {
            let Some(dir) = rfd::FileDialog::new().pick_folder() else {
                return;
            };
            let dir = dir.to_string_lossy().to_string();
            {
                let mut state = state.borrow_mut();
                if let Some(transfer) = state.transfers.get_mut(&(id as i64)) {
                    transfer.dir = Some(dir.clone());
                    transfer.status = TransferStatus::Ready;
                }
            }
            send(
                &commands,
                Envelope::TransferAccept(TransferAccept { id: id as i64, dir }),
            );
            if let Some(ui) = weak.upgrade() {
                ui.set_show_incoming(false);
                rebind_all(&ui, &state.borrow());
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_decline_incoming(move |id| {
            {
                let mut state = state.borrow_mut();
                if let Some(transfer) = state.transfers.get_mut(&(id as i64)) {
                    transfer.status = TransferStatus::Canceled;
                }
            }
            send(
                &commands,
                Envelope::TransferCancel(TransferRef { id: id as i64 }),
            );
            if let Some(ui) = weak.upgrade() {
                ui.set_show_incoming(false);
                rebind_all(&ui, &state.borrow());
            }
        });
    }

    {
        let state = state.clone();
        let commands = commands.clone();
        let weak = weak.clone();
        ui.on_files_dropped(move |data| {
            if let Ok(paths) = data.file_paths() {
                let paths: Vec<PathBuf> = paths.map(Path::to_path_buf).collect();
                if !paths.is_empty() {
                    add_paths(&state, &commands, weak.upgrade().as_ref(), paths, demo);
                }
            }
        });
    }

    ui.on_quit(|| {
        let _ = slint::quit_event_loop();
    });
}

fn send(commands: &Option<Sender<Envelope>>, envelope: Envelope) {
    if let Some(commands) = commands {
        let _ = commands.send(envelope);
    }
}

fn add_paths(
    state: &Rc<RefCell<ViewState>>,
    commands: &Option<Sender<Envelope>>,
    ui: Option<&AppWindow>,
    paths: Vec<PathBuf>,
    demo: bool,
) {
    if demo {
        let Ok(staged) = lingo_core::paths::scan_paths(&paths) else {
            return;
        };
        let mut state = state.borrow_mut();
        let note = state.app.staging.note.clone();
        let mut entries: BTreeMap<String, u64> = state
            .app
            .staging
            .manifest
            .iter()
            .map(|entry| (entry.path.clone(), entry.size))
            .collect();
        entries.extend(
            staged
                .manifest
                .iter()
                .map(|entry| (entry.path.clone(), entry.size)),
        );
        state.app.staging.manifest = entries
            .into_iter()
            .map(|(path, size)| ManifestEntry { path, size })
            .collect();
        state.app.staging.path_map.extend(staged.path_map);
        state.app.staging.note = note;
        if let Some(ui) = ui {
            rebind_all(ui, &state);
        }
    } else {
        send(
            commands,
            Envelope::StagingAdd(StagingAdd {
                paths: paths
                    .iter()
                    .map(|path| path.to_string_lossy().to_string())
                    .collect(),
            }),
        );
    }
}

fn apply_envelope(ui: &AppWindow, state: &mut ViewState, envelope: Envelope) {
    match envelope {
        Envelope::AppState(app) => state.app = app,
        Envelope::StagingSet(staging) => state.app.staging = staging,
        Envelope::SetPeer(device) => {
            state.devices.insert(device.id, device);
        }
        Envelope::TransferSet(transfer) => {
            state.transfers.insert(transfer.id, transfer);
        }
        Envelope::LinkSet(link) => {
            if link.did.is_empty() {
                toast(
                    ui,
                    "Link broken",
                    "This link doesn't lead anywhere.",
                    "warning",
                );
            } else {
                toast(ui, "Link ready", "The device was added.", "success");
            }
        }
        Envelope::ApiBadRequest(err) => toast(ui, "Can't continue", &err.msg, "error"),
        Envelope::LocalBadDevice(device) => toast(
            ui,
            "Unsupported device",
            format!("The other device runs version {}.", device.version),
            "warning",
        ),
        Envelope::NudgeTx(_) | Envelope::NudgeRx(_) => toast(
            ui,
            "Missed transfer",
            "Make sure both devices are active at the same time.",
            "warning",
        ),
        Envelope::ClientExit(_) => toast(
            ui,
            "Agent stopped",
            "The transfer engine exited. Reopen Lingo to continue.",
            "error",
        ),
        _ => {}
    }
}

fn rebind_all(ui: &AppWindow, state: &ViewState) {
    ui.set_page(state.page);
    ui.set_owner(state.app.avatar.owner.clone().into());
    ui.set_device_name(state.app.avatar.name.clone().into());
    ui.set_variant(state.app.avatar.variant.clone().into());
    ui.set_avatar_color(state.app.avatar.color.clone().into());
    ui.set_setup(state.app.setup);
    ui.set_note(state.app.staging.note.clone().into());
    ui.set_has_staging(!state.app.staging.is_empty());
    ui.set_staging_summary(staging_summary(&state.app.staging).into());
    ui.set_background_mode(state.background_mode);
    ui.set_notifications(state.notifications);
    ui.set_link_code(state.link_code.clone().into());

    let remote = state.app.private_remote_state.as_ref();
    let (account_label, guest) = match remote {
        Some(remote) => match &remote.account {
            Some(account) => (account.email.clone(), false),
            None => ("Guest mode".to_string(), true),
        },
        None if state.app.setup_account => ("Guest mode".to_string(), true),
        None => ("Offline".to_string(), false),
    };
    ui.set_account_label(account_label.into());
    ui.set_guest(guest);
    ui.set_online(state.app.setup_account);

    let mut selected = state.selected_device;
    if !state.devices.contains_key(&(selected as i64)) {
        selected = state
            .devices
            .keys()
            .next()
            .copied()
            .unwrap_or(-1)
            .try_into()
            .unwrap_or(-1);
    }
    ui.set_selected_device(selected);

    let devices: Vec<DeviceItem> = state.devices.values().map(device_item).collect();
    ui.set_devices(ModelRc::new(VecModel::from(devices)));

    let staging: Vec<StagingItem> = state
        .app
        .staging
        .manifest
        .iter()
        .map(|entry| StagingItem {
            name: entry
                .path
                .rsplit('/')
                .next()
                .unwrap_or(&entry.path)
                .to_string()
                .into(),
            meta: format_size(entry.size).into(),
            kind: if entry.path.contains('/') {
                "▸"
            } else {
                "·"
            }
            .into(),
        })
        .collect();
    ui.set_staging(ModelRc::new(VecModel::from(staging)));

    ui.set_inbox(ModelRc::new(VecModel::from(transfer_items(
        state,
        Role::Dst,
    ))));
    ui.set_sent(ModelRc::new(VecModel::from(transfer_items(
        state,
        Role::Src,
    ))));

    let incoming = state.transfers.values().find(|transfer| {
        transfer.role == Role::Dst
            && !transfer.removed
            && transfer.dir.is_none()
            && transfer.status == TransferStatus::Pending
    });
    if let Some(transfer) = incoming {
        let peer = peer_name(state, transfer.peer_id);
        ui.set_incoming_id(transfer.id as i32);
        ui.set_incoming_title(format!("{} wants to send files", peer).into());
        ui.set_incoming_subtitle(
            format!(
                "{} · {}",
                transfer.manifest.len(),
                format_size(transfer.total_size())
            )
            .into(),
        );
        ui.set_show_incoming(true);
    } else {
        ui.set_show_incoming(false);
    }
}

fn device_item(device: &Device) -> DeviceItem {
    let online = device.active || device.is_online;
    DeviceItem {
        id: device.id as i32,
        title: device.title().into(),
        subtitle: if online {
            "Available".to_string()
        } else {
            "Offline".to_string()
        }
        .into(),
        initials: device.avatar.initials().into(),
        online,
        active: device.active,
        tone: (device.id % 5) as i32,
    }
}

fn transfer_items(state: &ViewState, role: Role) -> Vec<TransferItem> {
    let mut transfers: Vec<&Transfer> = state
        .transfers
        .values()
        .filter(|transfer| transfer.role == role && !transfer.removed)
        .collect();
    transfers.sort_by_key(|transfer| std::cmp::Reverse(transfer.created_at));

    transfers
        .into_iter()
        .map(|transfer| {
            let status = transfer.effective_status();
            let terminal = status.is_terminal();
            let peer = peer_name(state, transfer.peer_id);
            let count = transfer.manifest.len();
            let subtitle = if let Some(err) = transfer.err.as_deref()
                && status == TransferStatus::Failed
            {
                format!("{peer} · {err}")
            } else {
                format!(
                    "{} · {} file{} · {}",
                    peer,
                    count,
                    if count == 1 { "" } else { "s" },
                    format_size(transfer.total_size())
                )
            };
            TransferItem {
                id: transfer.id as i32,
                title: if role == Role::Dst { "↓" } else { "↑" }.into(),
                subtitle: subtitle.into(),
                status: status_label(status, role).into(),
                progress: transfer.progress(),
                speed: transfer
                    .speed
                    .map(|speed| format!("{}/s", format_size(speed)))
                    .unwrap_or_default()
                    .into(),
                tone: status_tone(status),
                can_pause: !terminal && transfer.status == TransferStatus::Ready,
                can_resume: !terminal && transfer.status == TransferStatus::Paused,
                can_cancel: !terminal,
                can_remove: terminal,
                can_delete: role == Role::Dst
                    && status == TransferStatus::Complete
                    && transfer.dir.is_some(),
                can_open: role == Role::Dst && transfer.dir.is_some(),
            }
        })
        .collect()
}

fn peer_name(state: &ViewState, peer_id: i64) -> String {
    state
        .devices
        .get(&peer_id)
        .map(Device::title)
        .unwrap_or_else(|| "Unknown device".to_string())
}

fn status_label(status: TransferStatus, role: Role) -> &'static str {
    match status {
        TransferStatus::Complete => "Complete",
        TransferStatus::Canceled => "Canceled",
        TransferStatus::Failed => "Failed",
        TransferStatus::Ready if role == Role::Src => "Transferring",
        TransferStatus::Ready => "Ready",
        TransferStatus::Pending if role == Role::Src => "Waiting",
        TransferStatus::Pending => "Incoming",
        TransferStatus::Paused => "Paused",
    }
}

fn status_tone(status: TransferStatus) -> i32 {
    match status {
        TransferStatus::Complete => 1,
        TransferStatus::Canceled | TransferStatus::Failed => 3,
        TransferStatus::Paused | TransferStatus::Pending => 2,
        TransferStatus::Ready => 0,
    }
}

fn staging_summary(staging: &Staging) -> String {
    if staging.manifest.is_empty() {
        "No files staged".to_string()
    } else {
        format!(
            "{} file{} · {}",
            staging.manifest.len(),
            if staging.manifest.len() == 1 { "" } else { "s" },
            format_size(staging.total_size())
        )
    }
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn toast(ui: &AppWindow, title: impl Into<String>, body: impl Into<String>, level: &str) {
    let title: String = title.into();
    let body: String = body.into();
    ui.set_toast_title(title.into());
    ui.set_toast_body(body.into());
    ui.set_toast_level(level.into());
    ui.set_has_toast(true);
    let weak = ui.as_weak();
    Timer::single_shot(Duration::from_secs(4), move || {
        if let Some(ui) = weak.upgrade() {
            ui.set_has_toast(false);
        }
    });
}

fn open_in_finder(path: &str) {
    let _ = std::process::Command::new("open")
        .arg("-R")
        .arg(path)
        .spawn();
}

fn tray_icon() -> slint::Image {
    let mut buffer = slint::SharedPixelBuffer::<slint::Rgba8Pixel>::new(22, 22);
    let pixels = buffer.make_mut_slice();
    let mut fill = |x: usize, y: usize| {
        if x < 22 && y < 22 {
            pixels[y * 22 + x] = slint::Rgba8Pixel {
                r: 0,
                g: 0,
                b: 0,
                a: 255,
            };
        }
    };
    for y in 4..18 {
        for x in 5..8 {
            fill(x, y);
        }
    }
    for y in 4..7 {
        for x in 8..14 {
            fill(x, y);
        }
    }
    for y in 15..18 {
        for x in 8..14 {
            fill(x, y);
        }
    }
    for y in 7..15 {
        for x in 14..17 {
            fill(x, y);
        }
    }
    slint::Image::from_rgba8(buffer)
}

fn seed_gallery(ui: &AppWindow, state: &mut ViewState) {
    state.page = 0;
    state.selected_device = 1;
    state.background_mode = true;
    state.app = AppState {
        did: DeviceId::from_public_key(&[7u8; 32]),
        avatar: Avatar {
            owner: "Ken".into(),
            name: "Macbook".into(),
            variant: "macbook".into(),
            color: "green".into(),
        },
        setup: true,
        setup_account: true,
        setup_available: true,
        private_remote_state: None,
        staging: Staging {
            manifest: vec![
                ManifestEntry {
                    path: "Studio shoot/hero.jpg".into(),
                    size: 8_400_000,
                },
                ManifestEntry {
                    path: "Studio shoot/notes.txt".into(),
                    size: 1_240,
                },
                ManifestEntry {
                    path: "contract.pdf".into(),
                    size: 412_000,
                },
            ],
            path_map: BTreeMap::new(),
            note: "Photos from the studio shoot. The contract is in the root.".into(),
        },
        push_token: String::new(),
        port: 0,
        live: true,
    };

    state.devices = [
        lingo_core::service::demo_device(1, 11, "Ada", "Studio"),
        lingo_core::service::demo_device(2, 12, "Magnus", "Laptop"),
        lingo_core::service::demo_device(3, 13, "Jessica", "iMac"),
    ]
    .into_iter()
    .map(|device| (device.id, device))
    .collect();
    if let Some(device) = state.devices.get_mut(&2) {
        device.active = false;
        device.is_online = false;
    }
    if let Some(device) = state.devices.get_mut(&1) {
        device.active = true;
        device.is_online = true;
    }

    let now = now_micros();
    let transfers = vec![
        Transfer {
            id: 1,
            tid: Default::default(),
            role: Role::Src,
            created_at: now,
            status_at: now,
            manifest: vec![ManifestEntry {
                path: "keynote.pdf".into(),
                size: 2_400_000,
            }],
            note: "Slides for Monday.".into(),
            removed: false,
            deleted: false,
            offset: 2_400_000,
            peer_id: 1,
            path_map: BTreeMap::new(),
            dir: None,
            err: None,
            status: TransferStatus::Complete,
            peer_status: TransferStatus::Complete,
            duration: 1_800_000,
            speed: Some(1_400_000),
        },
        Transfer {
            id: 2,
            tid: Default::default(),
            role: Role::Dst,
            created_at: now - 60_000_000,
            status_at: now,
            manifest: vec![
                ManifestEntry {
                    path: "Reference/angles.png".into(),
                    size: 5_200_000,
                },
                ManifestEntry {
                    path: "Reference/brief.md".into(),
                    size: 8_100,
                },
            ],
            note: "Angles for the cover.".into(),
            removed: false,
            deleted: false,
            offset: 0,
            peer_id: 3,
            path_map: BTreeMap::new(),
            dir: Some("/Users/ken/Downloads".into()),
            err: None,
            status: TransferStatus::Ready,
            peer_status: TransferStatus::Ready,
            duration: 0,
            speed: Some(3_100_000),
        },
        Transfer {
            id: 3,
            tid: Default::default(),
            role: Role::Src,
            created_at: now - 120_000_000,
            status_at: now,
            manifest: vec![ManifestEntry {
                path: "archive.zip".into(),
                size: 120_000_000,
            }],
            note: String::new(),
            removed: false,
            deleted: false,
            offset: 48_000_000,
            peer_id: 2,
            path_map: BTreeMap::new(),
            dir: None,
            err: None,
            status: TransferStatus::Paused,
            peer_status: TransferStatus::Paused,
            duration: 4_200_000,
            speed: None,
        },
    ];
    state.transfers = transfers.into_iter().map(|t| (t.id, t)).collect();
    rebind_all(ui, state);
}
