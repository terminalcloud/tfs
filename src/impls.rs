// Copyright (C) 2016 Cloudlabs, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use {Storage, Cache, VolumeName, VolumeMetadata, ContentId, Snapshot};

impl Storage for Box<Storage> {
    fn snapshot(&self, volume: &VolumeName, snapshot: Snapshot) -> ::Result<()> {
        (**self).snapshot(volume, snapshot)
    }

    fn get_snapshot(&self, name: &VolumeName) -> ::Result<Snapshot> {
        (**self).get_snapshot(name)
    }

    fn get_metadata(&self, volume: &VolumeName) -> ::Result<VolumeMetadata> {
        (**self).get_metadata(volume)
    }

    fn create(&self, id: ContentId, data: &[u8]) -> ::Result<()> {
        (**self).create(id, data)
    }

    fn delete(&self, id: ContentId) -> ::Result<()> {
        (**self).delete(id)
    }
}

impl Cache for Box<Storage> {
    fn read(&self, id: ContentId, buf: &mut [u8]) -> ::Result<()> {
        (**self).read(id, buf)
    }
}

