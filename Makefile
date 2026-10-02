.PHONY: owner-check owner-test owner-fmt
owner-check:
	cargo check -p compio-driver
	cargo check -p compio-driver --features io-uring-sqe128,io-uring-cqe32
owner-test:
	cargo test -p compio-driver --test owner_lane
owner-fmt:
	rustfmt --edition 2024 --config skip_children=true compio-driver/src/lib.rs compio-driver/src/buffer_pool.rs compio-driver/src/sys/driver/iour/mod.rs compio-driver/src/sys/driver/mod.rs compio-driver/src/sys/mod.rs compio-driver/tests/owner_lane.rs

.PHONY: iocp-check iocp-test iocp-fmt
iocp-check:
	cargo check -p compio-driver --lib --tests --target x86_64-pc-windows-msvc
iocp-test:
	cargo test -p compio-driver --lib connection_reset_is_not_eof
iocp-fmt:
	rustfmt --edition 2024 --config skip_children=true compio-driver/src/sys/driver/iocp/cp/mod.rs compio-driver/src/sys/pal/windows/mod.rs
