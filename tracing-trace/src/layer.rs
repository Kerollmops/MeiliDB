use std::alloc::{GlobalAlloc, System};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use std::ops::ControlFlow;
use std::sync::RwLock;

use stats_alloc::StatsAlloc;
use tracing::span::{Attributes, Id as TracingId};
use tracing::{Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use crate::entry::{
    Entry, NewCallsite, NewSpan, NewThread, ResourceId, SpanClose, SpanEnter, SpanExit, SpanId,
};
use crate::{Error, Trace};

/// Layer that measures the time spent in spans.
pub struct TraceLayer<A: GlobalAlloc + 'static = System> {
    sender: std::sync::mpsc::Sender<Entry>,
    callsites: RwLock<HashMap<OpaqueIdentifier, ResourceId>>,
    start_time: std::time::Instant,
    memory_allocator: Option<&'static StatsAlloc<A>>,
}

impl<W: Write> Trace<W> {
    pub fn new(writer: W) -> (Self, TraceLayer<System>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let trace = Trace { writer, receiver };
        let layer = TraceLayer {
            sender,
            callsites: Default::default(),
            start_time: std::time::Instant::now(),
            memory_allocator: None,
        };
        (trace, layer)
    }

    pub fn with_stats_alloc<A: GlobalAlloc>(
        writer: W,
        stats_alloc: &'static StatsAlloc<A>,
    ) -> (Self, TraceLayer<A>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let trace = Trace { writer, receiver };
        let layer = TraceLayer {
            sender,
            callsites: Default::default(),
            start_time: std::time::Instant::now(),
            memory_allocator: Some(stats_alloc),
        };
        (trace, layer)
    }

    pub fn receive(&mut self) -> Result<ControlFlow<(), ()>, Error> {
        let Ok(entry) = self.receiver.recv() else {
            return Ok(ControlFlow::Break(()));
        };
        self.write(entry)?;
        Ok(ControlFlow::Continue(()))
    }

    pub fn write(&mut self, entry: Entry) -> Result<(), Error> {
        Ok(serde_json::ser::to_writer(&mut self.writer, &entry)?)
    }

    pub fn try_receive(&mut self) -> Result<ControlFlow<(), ()>, Error> {
        let Ok(entry) = self.receiver.try_recv() else {
            return Ok(ControlFlow::Break(()));
        };
        self.write(entry)?;
        Ok(ControlFlow::Continue(()))
    }

    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()
    }
}

#[derive(PartialEq, Eq, Hash)]
enum OpaqueIdentifier {
    Thread(std::thread::ThreadId),
    Call(tracing::callsite::Identifier),
}

impl<A: GlobalAlloc> TraceLayer<A> {
    fn resource_id(&self, opaque: OpaqueIdentifier) -> Option<ResourceId> {
        self.callsites.read().unwrap().get(&opaque).copied()
    }

    fn register_resource_id(&self, opaque: OpaqueIdentifier) -> ResourceId {
        let mut map = self.callsites.write().unwrap();
        let len = map.len();
        *map.entry(opaque).or_insert(ResourceId(len))
    }

    fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    fn send(&self, entry: Entry) {
        // we never care that the other end hanged on us
        let _ = self.sender.send(entry);
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> ResourceId {
        let call_id = self.register_resource_id(OpaqueIdentifier::Call(metadata.callsite()));

        let module_path = metadata.module_path();
        let file = metadata.file();
        let line = metadata.line();
        let name = metadata.name();
        let target = metadata.target();

        self.send(Entry::NewCallsite(NewCallsite {
            call_id,
            module_path: module_path.map(Cow::Borrowed),
            file: file.map(Cow::Borrowed),
            line,
            name: Cow::Borrowed(name),
            target: Cow::Borrowed(target),
        }));
        call_id
    }

    fn register_thread(&self) -> ResourceId {
        let thread_id = std::thread::current().id();
        let name = std::thread::current().name().map(ToOwned::to_owned);
        let thread_id = self.register_resource_id(OpaqueIdentifier::Thread(thread_id));
        self.send(Entry::NewThread(NewThread { thread_id, name }));
        thread_id
    }
}

impl<S, A> Layer<S> for TraceLayer<A>
where
    S: Subscriber,
    A: GlobalAlloc,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &TracingId, _ctx: Context<'_, S>) {
        let call_id = self
            .resource_id(OpaqueIdentifier::Call(attrs.metadata().callsite()))
            .unwrap_or_else(|| self.register_callsite(attrs.metadata()));

        let thread_id = self
            .resource_id(OpaqueIdentifier::Thread(std::thread::current().id()))
            .unwrap_or_else(|| self.register_thread());

        let parent_id = attrs
            .parent()
            .cloned()
            .or_else(|| tracing::Span::current().id())
            .map(|id| SpanId::from(&id));

        self.send(Entry::NewSpan(NewSpan { id: id.into(), call_id, parent_id, thread_id }));
    }

    fn on_enter(&self, id: &TracingId, _ctx: Context<'_, S>) {
        self.send(Entry::SpanEnter(SpanEnter {
            id: id.into(),
            time: self.elapsed(),
            memory: self.memory_allocator.map(|ma| ma.stats().into()),
        }))
    }

    fn on_exit(&self, id: &TracingId, _ctx: Context<'_, S>) {
        self.send(Entry::SpanExit(SpanExit {
            id: id.into(),
            time: self.elapsed(),
            memory: self.memory_allocator.map(|ma| ma.stats().into()),
        }))
    }

    fn on_close(&self, id: TracingId, _ctx: Context<'_, S>) {
        self.send(Entry::SpanClose(SpanClose { id: Into::into(&id), time: self.elapsed() }))
    }
}
