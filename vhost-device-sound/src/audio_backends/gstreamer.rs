use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, RwLock,
    },
};

use gstreamer::{
    self as gs,
    glib::{
        object::{Cast, ObjectExt},
        MainLoop,
    },
    prelude::{ElementExt, GstBinExt, GstBinExtManual},
};
use gstreamer_app::{self as gs_app};
use thiserror::Error as ThisError;

use super::AudioBackend;
use crate::{stream::PCMState, virtio_sound, Direction, Error, Stream};

pub fn gstreamer_worker(
    streams: Arc<RwLock<Vec<Stream>>>,
    handles: Arc<RwLock<HashMap<u32, GstStreamHandle>>>,
    receiver: &Receiver<bool>,
    stream_id: usize,
) -> std::result::Result<(), Error> {
    let direction = streams.read().unwrap()[stream_id].direction;

    loop {
        // blocking until new signal is sent
        // If the recv() returns `Ok(false)` or an error, terminate this worker thread.
        let Ok(do_work) = receiver.recv() else {
            return Ok(());
        };

        if do_work {
            println!("yes receied!");
            let has_buffers = || -> bool {
                let lck = streams.read().unwrap();
                !lck[stream_id].requests.is_empty()
                    && matches!(lck[stream_id].state, PCMState::Start)
            };

            'empty_buffers: while has_buffers() {
                println!("so has buffer right?");
                let should_continue = {
                    let handles_lock = handles.read().unwrap();
                    let handle = handles_lock
                        .get(&(stream_id as u32))
                        .ok_or_else(|| Error::StreamWithIdNotFound(stream_id as u32))?;

                    match direction {
                        Direction::Output => {
                            // Reading from the guest and push to the appsrc
                            if let Some(ref appsrc) = handle.appsrc {
                                write_samples_gstreamer(&streams, stream_id, appsrc)?
                            } else {
                                println!("or might be no appsrfc");
                                log::error!("No appsrc available for output stream {}", stream_id);
                                false
                            }
                        }
                        Direction::Input => {
                            // Reading from appsink and write to guest
                            // TODO
                            false
                        }
                    }
                };

                if !should_continue {
                    break 'empty_buffers;
                }
            }
        }
    }
}
//  samples from guest to GStreamer appsrc (playback)
fn write_samples_gstreamer(
    streams: &Arc<RwLock<Vec<Stream>>>,
    stream_id: usize,
    appsrc: &gstreamer_app::AppSrc,
) -> Result<bool, Error> {
    println!(
        "write_samples: attempting to write samples for stream {}",
        stream_id
    );
    let mut has_data = false;

    let mut streams_lock = streams.write().unwrap();
    let stream = &mut streams_lock[stream_id];

    while let Some(req) = stream.requests.front_mut() {
        println!("write_samples: processing request for stream {}", stream_id);
        let remaining = req.len().saturating_sub(req.pos);
        if remaining == 0 {
            stream.requests.pop_front().unwrap();
            continue;
        }

        let mut buffer = vec![0_u8; remaining];
        let read_bytes = match req.read_output(&mut buffer) {
            Ok(n) => n,
            Err(err) => {
                log::error!("Could not read TX request from guest: {}", err);
                stream.requests.pop_front();
                return Ok(true);
            }
        };

        if read_bytes > 0 {
            println!(
                "write_samples: read {} bytes for stream {}",
                read_bytes, stream_id
            );
            buffer.truncate(read_bytes as usize);
            let gst_buffer = gs::Buffer::from_slice(buffer);
            match appsrc.push_buffer(gst_buffer) {
                Ok(_) => {
                    req.pos += read_bytes as usize;
                    has_data = true;
                }
                Err(_) => {
                    // req.pos += read_bytes as usize;
                    return Ok(false);
                }
            }
        }

        if req.pos >= req.len() {
            println!("completed!");
            stream.requests.pop_front().unwrap();
        } else {
            break;
        }
    }

    Ok(has_data)
}

pub struct GStreamerBackend {
    pub senders: Vec<Sender<bool>>,
    pub streams: Arc<RwLock<Vec<Stream>>>,
    pub handles: Arc<RwLock<HashMap<u32, GstStreamHandle>>>,
    pub main_loop: Option<MainLoop>,
    pub loop_handle: Option<std::thread::JoinHandle<()>>,
}

pub struct GstStreamHandle {
    pub pipeline: gs::Pipeline,
    pub appsrc: Option<gs_app::AppSrc>,   // for playback
    pub appsink: Option<gs_app::AppSink>, // for capture
}

impl GStreamerBackend {
    pub fn new(streams: Arc<RwLock<Vec<Stream>>>) -> Self {
        if let Err(e) = gs::init() {
            panic!("Failed to initialize GStreamer: {}", e);
        }
        let streams_no = streams.read().unwrap().len();
        let mut senders = Vec::with_capacity(streams_no);
        let main_loop = MainLoop::new(None, false);
        let thread_loop = main_loop.clone();
        let handle = std::thread::spawn(move || {
            thread_loop.run();
            log::trace!("Gstreamer main loop thread finished");
        });
        let stream_handles = Arc::new(RwLock::new(HashMap::new()));
        for i in 0..streams_no {
            let (sender, receiver) = mpsc::channel();
            senders.push(sender);
            let streams_clone = streams.clone();
            let handles_clone = stream_handles.clone();
            // gstreamer worker for all the stream that comes along
            std::thread::spawn(move || {
                while let Err(err) =
                    gstreamer_worker(streams_clone.clone(), handles_clone.clone(), &receiver, i)
                {
                    log::error!(
                        "Worker thread exited with error: {}, sleeping for 500ms",
                        err
                    );
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
            });
        }
        log::trace!("Gstreamer backend initialized with main loop");
        println!("the backend is up and running");
        Self {
            streams,
            handles: stream_handles,
            main_loop: Some(main_loop),
            senders,
            loop_handle: Some(handle),
        }
    }
    fn build_pipeline(id: u32, direction: crate::Direction) -> Result<gs::Pipeline, GsError> {
        println!("preparing pipeline");
        let pipeline = gstreamer::Pipeline::new();

        match direction {
            Direction::Input => {
                let audiosrc = gstreamer::ElementFactory::make("autoaudiosrc")
                    .name(format!("src{}", id))
                    .build()
                    .unwrap();

                let queue1 = gstreamer::ElementFactory::make("queue")
                    .property("max-size-buffers", 10u32)
                    .build()
                    .unwrap();

                let convert = gstreamer::ElementFactory::make("audioconvert")
                    .build()
                    .unwrap();

                let resample = gstreamer::ElementFactory::make("audioresample")
                    .build()
                    .unwrap();

                let appsink = gstreamer::ElementFactory::make("appsink")
                    .name(format!("sink{}", id))
                    .build()
                    .unwrap();

                pipeline
                    .add_many([&audiosrc, &queue1, &convert, &resample, &appsink])
                    .unwrap();

                gstreamer::Element::link_many([&audiosrc, &queue1, &convert, &resample, &appsink])
                    .unwrap();
            }
            Direction::Output => {
                let appsrc = gstreamer::ElementFactory::make("appsrc")
                    .name(format!("src{}", id))
                    .build()
                    .unwrap();

                let queue1 = gstreamer::ElementFactory::make("queue")
                    .property("max-size-buffers", 20u32)
                    .build()
                    .unwrap();

                let convert = gstreamer::ElementFactory::make("audioconvert")
                    .build()
                    .unwrap();

                let resample = gstreamer::ElementFactory::make("audioresample")
                    .build()
                    .unwrap();

                let audiosink = gstreamer::ElementFactory::make("autoaudiosink")
                    .name(format!("sink{}", id))
                    .build()
                    .unwrap();

                // or maybe forcing ALSA
                // let audiosink = gstreamer::ElementFactory::make("alsasink")
                //     .name(format!("sink{}", id))
                //     .property("device", "hw:0,0")
                //     .build()
                //     .map_err(|_| GstError::new("Failed to create alsasink"))?;

                pipeline
                    .add_many([&appsrc, &queue1, &convert, &resample, &audiosink])
                    .unwrap();

                gstreamer::Element::link_many([&appsrc, &queue1, &convert, &resample, &audiosink])
                    .unwrap();
            }
        }

        log::trace!("pipeline: {:?}", pipeline);
        Ok(pipeline)
    }

    fn configure_appsrc_for_stream(
        appsrc: &gstreamer_app::AppSrc,
        stream: &Stream,
    ) -> crate::Result<()> {
        let format_str = match stream.params.format {
            virtio_sound::VIRTIO_SND_PCM_FMT_S8 => "S8",
            virtio_sound::VIRTIO_SND_PCM_FMT_U8 => "U8",
            virtio_sound::VIRTIO_SND_PCM_FMT_S16 => "S16LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U16 => "U16LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_S24_3 => "S24LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U24_3 => "U24LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_S24 => "S24_32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U24 => "U24_32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_S32 => "S32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U32 => "U32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_FLOAT => "F32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_FLOAT64 => "F64LE",
            _ => {
                return Err(Error::UnexpectedAudioBackendError(
                    format!("Unsupported audio format: {:?}", stream.params.format).into(),
                ))
            }
        };

        let sample_rate = match stream.params.rate {
            virtio_sound::VIRTIO_SND_PCM_RATE_5512 => 5512,
            virtio_sound::VIRTIO_SND_PCM_RATE_8000 => 8000,
            virtio_sound::VIRTIO_SND_PCM_RATE_11025 => 11025,
            virtio_sound::VIRTIO_SND_PCM_RATE_16000 => 16000,
            virtio_sound::VIRTIO_SND_PCM_RATE_22050 => 22050,
            virtio_sound::VIRTIO_SND_PCM_RATE_32000 => 32000,
            virtio_sound::VIRTIO_SND_PCM_RATE_44100 => 44100,
            virtio_sound::VIRTIO_SND_PCM_RATE_48000 => 48000,
            virtio_sound::VIRTIO_SND_PCM_RATE_64000 => 64000,
            virtio_sound::VIRTIO_SND_PCM_RATE_88200 => 88200,
            virtio_sound::VIRTIO_SND_PCM_RATE_96000 => 96000,
            virtio_sound::VIRTIO_SND_PCM_RATE_176400 => 176400,
            virtio_sound::VIRTIO_SND_PCM_RATE_192000 => 192000,
            virtio_sound::VIRTIO_SND_PCM_RATE_12000 => 12000,
            virtio_sound::VIRTIO_SND_PCM_RATE_24000 => 24000,
            _ => {
                return Err(Error::UnexpectedAudioBackendError(
                    format!("Unsupported sample rate: {:?}", stream.params.rate).into(),
                ))
            }
        };

        let channels = i32::from(stream.params.channels);

        let caps = gs::Caps::builder("audio/x-raw")
            .field("format", format_str)
            .field("rate", sample_rate)
            .field("channels", channels)
            .field("layout", "interleaved")
            .build();

        appsrc.set_caps(Some(&caps));
        appsrc.set_property("is-live", true);
        appsrc.set_property("do-timestamp", true);
        appsrc.set_property("format", gs::Format::Time);

        appsrc.set_property("block", true);
        appsrc.set_property("is-live", true);
        appsrc.set_property("format", gs::Format::Time);

        let frame_size = Self::get_frame_size(stream.params.format as u32);
        let bytes_per_sec = sample_rate as u64 * channels as u64 * frame_size as u64;

        // manual configuration of 50_000 microseconds as the chunk size
        // dynamic adaptation is required
        let base_buffer_duration_us = 50_000;
        let buffer_bytes = bytes_per_sec * base_buffer_duration_us / 1_000_000;

        let max_buffers = 20u64;
        // window size
        let max_bytes = buffer_bytes * 8;

        appsrc.set_property("max-bytes", max_bytes);
        appsrc.set_property("max-buffers", max_buffers);

        let max_time_ns = 2_000_000_000u64;

        appsrc.set_property("max-time", max_time_ns);

        appsrc.set_property("min-latency", 10_000_000i64);
        appsrc.set_property("max-latency", 500_000_000i64);

        appsrc.set_property("do-timestamp", false);

        println!("Prepare: Appsrc Caps: {:?}", appsrc.caps());
        println!("Prepare Appsrc Properties configured for stream");

        Ok(())
    } // Helper function to configure AppSink for input streams
    fn configure_appsink_for_stream(
        app_sink: &gstreamer_app::AppSink,
        stream: &Stream,
    ) -> crate::Result<()> {
        let format_str = match stream.params.format {
            virtio_sound::VIRTIO_SND_PCM_FMT_S8 => "S8",
            virtio_sound::VIRTIO_SND_PCM_FMT_U8 => "U8",
            virtio_sound::VIRTIO_SND_PCM_FMT_S16 => "S16LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U16 => "U16LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_S24_3 => "S24LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U24_3 => "U24LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_S24 => "S24_32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U24 => "U24_32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_S32 => "S32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_U32 => "U32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_FLOAT => "F32LE",
            virtio_sound::VIRTIO_SND_PCM_FMT_FLOAT64 => "F64LE",
            _ => {
                return Err(Error::UnexpectedAudioBackendError(
                    format!("Unsupported audio format: {:?}", stream.params.format).into(),
                ))
            }
        };

        let sample_rate = match stream.params.rate {
            virtio_sound::VIRTIO_SND_PCM_RATE_5512 => 5512,
            virtio_sound::VIRTIO_SND_PCM_RATE_8000 => 8000,
            virtio_sound::VIRTIO_SND_PCM_RATE_11025 => 11025,
            virtio_sound::VIRTIO_SND_PCM_RATE_16000 => 16000,
            virtio_sound::VIRTIO_SND_PCM_RATE_22050 => 22050,
            virtio_sound::VIRTIO_SND_PCM_RATE_32000 => 32000,
            virtio_sound::VIRTIO_SND_PCM_RATE_44100 => 44100,
            virtio_sound::VIRTIO_SND_PCM_RATE_48000 => 48000,
            virtio_sound::VIRTIO_SND_PCM_RATE_64000 => 64000,
            virtio_sound::VIRTIO_SND_PCM_RATE_88200 => 88200,
            virtio_sound::VIRTIO_SND_PCM_RATE_96000 => 96000,
            virtio_sound::VIRTIO_SND_PCM_RATE_176400 => 176400,
            virtio_sound::VIRTIO_SND_PCM_RATE_192000 => 192000,
            virtio_sound::VIRTIO_SND_PCM_RATE_12000 => 12000,
            virtio_sound::VIRTIO_SND_PCM_RATE_24000 => 24000,
            _ => {
                return Err(Error::UnexpectedAudioBackendError(
                    format!("Unsupported sample rate: {:?}", stream.params.rate).into(),
                ))
            }
        };

        let channels = i32::from(stream.params.channels);

        let caps = gs::Caps::builder("audio/x-raw")
            .field("format", format_str)
            .field("rate", sample_rate)
            .field("channels", channels)
            .field("layout", "interleaved")
            .build();

        app_sink.set_caps(Some(&caps));

        app_sink.set_property("max-buffers", 20u32);

        // Control buffer behavior
        app_sink.set_property("drop", false);
        app_sink.set_property("wait-on-eos", true);

        Ok(())
    }
    fn get_frame_size(format: u32) -> u32 {
        match format as u8 {
            virtio_sound::VIRTIO_SND_PCM_FMT_S8 | virtio_sound::VIRTIO_SND_PCM_FMT_U8 => 1_u32,
            virtio_sound::VIRTIO_SND_PCM_FMT_S16 | virtio_sound::VIRTIO_SND_PCM_FMT_U16 => 2_u32,
            virtio_sound::VIRTIO_SND_PCM_FMT_S24_3 | virtio_sound::VIRTIO_SND_PCM_FMT_U24_3 => {
                3_u32
            }
            virtio_sound::VIRTIO_SND_PCM_FMT_S24
            | virtio_sound::VIRTIO_SND_PCM_FMT_U24
            | virtio_sound::VIRTIO_SND_PCM_FMT_S32
            | virtio_sound::VIRTIO_SND_PCM_FMT_U32
            | virtio_sound::VIRTIO_SND_PCM_FMT_FLOAT => 4_u32,
            virtio_sound::VIRTIO_SND_PCM_FMT_FLOAT64 => 8_u32,
            _ => 4_u32,
        }
    }
}

impl Drop for GStreamerBackend {
    fn drop(&mut self) {
        if let Some(main_loop) = self.main_loop.take() {
            main_loop.quit();
        }

        if let Some(handle) = self.loop_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Error type for the Gstreamer backend
#[derive(Debug, ThisError)]
pub enum GsError {
    #[error("Stream with id {0} not found")]
    PipelineError(gs::glib::Error),
    // #[error("AppSrc error: {0}")]
    // AppSrcError(gs::glib::Error),
    // #[error("AppSink error: {0}")]
    // AppSinkError(gs::glib::Error),
}

impl AudioBackend for GStreamerBackend {
    fn read(&self, stream_id: u32) -> crate::Result<()> {
        let handler_guard = self
            .handles
            .read()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?;

        let stream_handle = handler_guard
            .get(&stream_id)
            .ok_or_else(|| crate::Error::StreamWithIdNotFound(stream_id))?;

        stream_handle
            .pipeline
            .set_state(gstreamer::State::Playing)
            .map_err(|e| {
                Error::UnexpectedAudioBackendError(
                    format!("Failed to start pipeline. Error: {e}").into(),
                )
            })?;

        let streams_guard = self
            .streams
            .read()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned streams lock".into()))?;

        let stream_index = stream_id as usize;
        if stream_index >= streams_guard.len() {
            return Err(Error::StreamWithIdNotFound(stream_id));
        }

        let stream_state = streams_guard[stream_index].state;
        drop(streams_guard);

        if matches!(stream_state, PCMState::Start | PCMState::Prepare) {
            if stream_index >= self.senders.len() {
                return Err(Error::UnexpectedAudioBackendError(
                    "Sender index out of bounds".into(),
                ));
            }

            self.senders[stream_index].send(true).map_err(|e| {
                Error::UnexpectedAudioBackendError(format!("Failed to send signal: {}", e).into())
            })?;
        } else {
            return Err(Error::Stream(crate::stream::Error::InvalidState(
                "read",
                stream_state,
            )));
        }

        Ok(())
    }

    fn stop(&self, stream_id: u32) -> crate::Result<()> {
        // debug!("gstreamer stop");
        let handler_guard = self
            .handles
            .write()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?;
        let stream_handle = handler_guard
            .get(&stream_id)
            .ok_or_else(|| crate::Error::StreamWithIdNotFound(stream_id))?;
        stream_handle
            .pipeline
            .set_state(gstreamer::State::Null)
            .map_err(|e| {
                Error::UnexpectedAudioBackendError(
                    format!("Failed to stop pipeline. Error : {e}").into(),
                )
            })?;
        Ok(())
    }

    fn write(&self, stream_id: u32) -> crate::Result<()> {
        println!("Write: Now Writing The Audio");

        let streams_guard = self
            .streams
            .read()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned streams lock".into()))?;

        let stream_index = stream_id as usize;
        if stream_index >= streams_guard.len() {
            return Err(Error::StreamWithIdNotFound(stream_id));
        }

        let stream_state = streams_guard[stream_index].state;
        println!("stream request: {:?}", streams_guard[stream_index].requests);
        drop(streams_guard);

        if matches!(stream_state, PCMState::Start | PCMState::Prepare) {
            if stream_index >= self.senders.len() {
                return Err(Error::UnexpectedAudioBackendError(
                    "Sender index out of bounds".into(),
                ));
            }

            self.senders[stream_index].send(true).map_err(|e| {
                Error::UnexpectedAudioBackendError(
                    format!("Failed to send write signal: {}", e).into(),
                )
            })?;
        } else {
            return Err(Error::Stream(crate::stream::Error::InvalidState(
                "write",
                stream_state,
            )));
        }

        Ok(())
    }

    fn start(&self, stream_id: u32) -> crate::Result<()> {
        println!("Starting GStreamer pipeline for stream {}", stream_id);

        let handler_guard = self
            .handles
            .write()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?;
        let stream_handle = handler_guard
            .get(&stream_id)
            .ok_or_else(|| crate::Error::StreamWithIdNotFound(stream_id))?;

        stream_handle
            .pipeline
            .set_state(gs::State::Playing)
            .map_err(|e| {
                Error::UnexpectedAudioBackendError(
                    format!("Failed to start pipeline: {:?}", e).into(),
                )
            })?;

        let mut streams_guard = self
            .streams
            .write()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?;
        let stream = streams_guard
            .get_mut(stream_id as usize)
            .ok_or(Error::StreamWithIdNotFound(stream_id))?;

        if matches!(stream.state, PCMState::Prepare) {
            stream.state.start().map_err(Error::Stream)?;
        }

        println!("Stream {} started", stream_id);

        self.senders[stream_id as usize].send(true).map_err(|e| {
            Error::UnexpectedAudioBackendError(format!("Failed to send write signal: {}", e).into())
        })?;

        Ok(())
    }

    fn prepare(&self, stream_id: u32) -> crate::Result<()> {
        let mut streams_guard = self
            .streams
            .write()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?;
        let stream = streams_guard
            .get_mut(stream_id as usize)
            .ok_or(Error::StreamWithIdNotFound(stream_id))?;

        if let Ok(handles) = self.handles.read() {
            if let Some(handle) = handles.get(&stream_id) {
                let (_, _, state) = handle.pipeline.state(gs::ClockTime::NONE);
                if state == gs::State::Playing || state == gs::State::Paused {
                    println!(
                        "Pipeline already exists and healthy for stream {}",
                        stream_id
                    );
                    return Ok(());
                }
            }
        }

        if matches!(stream.state, PCMState::Start) {
            println!("prepared function called even after the pcm state is start so returned");
            return Ok(());
        }

        let pipeline = Self::build_pipeline(stream_id, stream.direction)
            .map_err(|e| Error::UnexpectedAudioBackendError(Box::new(e)))?;

        println!("Prepare: Pipeline Built");

        let src_element = pipeline
            .by_name(&format!("src{}", stream_id))
            .ok_or_else(|| {
                Error::UnexpectedAudioBackendError(
                    format!("Element 'src{}' not found", stream_id).into(),
                )
            })?;
        let sink_element = pipeline
            .by_name(&format!("sink{}", stream_id))
            .ok_or_else(|| {
                Error::UnexpectedAudioBackendError(
                    format!("Element 'sink{}' not found", stream_id).into(),
                )
            })?;

        match stream.direction {
            Direction::Output => {
                let app_src = src_element
                    .downcast::<gstreamer_app::AppSrc>()
                    .map_err(|_| {
                        Error::UnexpectedAudioBackendError(
                            format!("Failed to downcast src{} to AppSrc", stream_id).into(),
                        )
                    })?;

                Self::configure_appsrc_for_stream(&app_src, stream).unwrap();

                let stream_handler = GstStreamHandle {
                    pipeline: pipeline.clone(),
                    appsrc: Some(app_src),
                    appsink: None,
                };

                self.handles
                    .write()
                    .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?
                    .insert(stream_id, stream_handler);
            }
            Direction::Input => {
                let app_sink = sink_element
                    .downcast::<gstreamer_app::AppSink>()
                    .map_err(|_| {
                        Error::UnexpectedAudioBackendError(
                            format!("Failed to downcast sink{} to AppSink", stream_id).into(),
                        )
                    })?;
                Self::configure_appsink_for_stream(&app_sink, stream).unwrap();

                let stream_handler = GstStreamHandle {
                    pipeline: pipeline.clone(),
                    appsrc: None,
                    appsink: Some(app_sink),
                };

                self.handles
                    .write()
                    .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?
                    .insert(stream_id, stream_handler);
            }
        }
        pipeline.set_state(gs::State::Ready).map_err(|e| {
            Error::UnexpectedAudioBackendError(
                format!("Failed to set pipeline to READY: {:?}", e).into(),
            )
        })?;
        stream.state.prepare().map_err(Error::Stream)?;

        println!("Prepare: Finished!");
        Ok(())
    }

    fn release(&self, _stream_id: u32) -> crate::Result<()> {
        Ok(())
    }
    fn set_parameters(
        &self,
        stream_id: u32,
        request: crate::virtio_sound::VirtioSndPcmSetParams,
    ) -> crate::Result<()> {
        let mut streams = self
            .streams
            .write()
            .map_err(|_| Error::UnexpectedAudioBackendError("Poisoned lock".into()))?;

        if let Some(st) = streams.get_mut(stream_id as usize) {
            st.state.set_parameters().map_err(Error::Stream)?;

            if !st.supports_format(request.format) || !st.supports_rate(request.rate) {
                return Err(Error::UnexpectedAudioBackendConfiguration);
            }

            st.params.buffer_bytes = request.buffer_bytes;
            st.params.period_bytes = request.period_bytes;
            st.params.features = request.features;
            st.params.channels = request.channels;
            st.params.format = request.format;
            st.params.rate = request.rate;
        } else {
            return Err(Error::StreamWithIdNotFound(stream_id));
        }
        println!("Set Parameter: Finished! Parameters");
        Ok(())
    }
    #[cfg(test)]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
