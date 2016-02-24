#![allow(dead_code, unused_variables)]
use {Cache, ContentId};

pub struct P2PCache {
    blah: usize
}

impl Cache for P2PCache {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> { unimplemented!() }
}

