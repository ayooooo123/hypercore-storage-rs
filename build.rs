fn main() {
    println!("cargo:rerun-if-changed=spec/hyperschema/schema.json");
    let opts = hyperschema::Options::new();
    hyperschema::build(
        "spec/hyperschema/schema.json",
        "hypercore_storage.rs",
        &opts,
    )
    .unwrap();
}
