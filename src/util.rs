#[cfg(test)]
pub mod test {
    use slog::Logger;

    pub fn discard_logger() -> Logger {
        use slog::{o, Discard, Drain};
        use slog_async::Async;

        let drain = Discard.fuse();
        let drain = Async::new(drain).build().fuse();

        Logger::root(drain, o!())
    }
}
