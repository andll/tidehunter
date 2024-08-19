pub struct Config {
    frag_size: u64,
    map_size: u64,
    large_table_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 256 * 1024 * 1024,
            map_size: 1024 * 1024,
            large_table_size: 64 * 1024,
        }
    }
}

impl Config {
    pub fn small() -> Self {
        Self {
            frag_size: 1024 * 1024,
            map_size: 16 * 1024,
            large_table_size: 256,
        }
    }
}
