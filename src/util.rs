#[cfg(test)]
pub mod test {
    use slog::{o,Drain, Logger, Discard};
    use slog_async::Async;

    pub fn logger() -> Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();

        Logger::root(drain, o!())
    }

    pub fn discard_logger() -> Logger {
        let drain = Discard.fuse();
        let drain = Async::new(drain).build().fuse();

        Logger::root(drain, o!())
    }
}
