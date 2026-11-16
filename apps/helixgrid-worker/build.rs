fn main() {
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("../../assets/branding/helixgrid.ico");
        res.set("ProductName", "HelixGrid");
        res.set("FileDescription", "HelixGrid Local Compute Worker");
        res.set("LegalCopyright", "Copyright (c) 2026 HelixGrid Authors");
        if let Err(err) = res.compile() {
            println!("cargo:warning=winresource failed: {err}");
        }
    }
}
