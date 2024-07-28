pc:
	cargo +nightly build --features="pc" --target="x86_64-unknown-linux-gnu"

embedded:
	cargo build --features="embedded"
