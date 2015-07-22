<?php
/**
 * ownCloud - rados
 *
 * @author Jörn Dreyer
 * @copyright 2015 Jörn Dreyer <jfd@owncloud.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU AFFERO GENERAL PUBLIC LICENSE
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU AFFERO GENERAL PUBLIC LICENSE for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this library.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

namespace OCA\Rados;

use OCP\Files\NotFoundException;
use OCP\Files\ObjectStore\IObjectStore;


class RadosStore implements IObjectStore {

	/**
	 * @var string
	 */
	private $pool;

	/**
	 * TODO don't hardcode pool, use params
	 * TODO check rados extensions are available
	 * @param $params
	 */
	public function __construct($params) {
		$this->pool = 'owncloud';
		if ( ! in_array("rados", stream_get_wrappers())) {
			stream_wrapper_register("rados", '\OCA\Rados\RadosStream');
		}
	}

	/**
	 * @return string the container name where objects are stored
	 */
	public function getStorageId() {
		return $this->pool;
	}


	/**
	 * @param string $urn the unified resource name used to identify the object
	 * @param resource $stream stream with the data to write
	 * @throws IOException when something goes wrong
	 */
	public function writeObject($urn, $stream) {
		$out = fopen('rados://'.$urn, 'w');
		stream_copy_to_stream($stream, $out);
	}

	/**
	 * @param $urn string the unified resource name used to identify the object
	 * @return void
	 * @throws IOException when something goes wrong
	 */
	public function deleteObject($urn) {
		if (!unlink('rados://'.$urn)) {
			throw new IOException('could not delete rados://'.$urn);
		}
	}

	/**
	 * @param $urn string the unified resource name used to identify the object
	 * @return resource stream with the read data
	 * @throws IOException when something goes wrong
	 * @throws NotFoundException when object does not exist
	 */
	public function readObject($urn) {
		$stream = fopen('rados://'.$urn, 'r+');
		if (!$stream) {
			throw new NotFoundException('rados://'.$urn.' not found');
		}
		return $stream;
	}
}
