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
	private $chunksize = 4194304;

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
		$part = 1;
		$written = 0;

		while(!feof($stream)) {
			$size = 0;
			$start = $size;
			$name = $urn.'_part'.$part;
			$out = fopen('rados://'.$name, 'w');
			$data = stream_get_contents($stream, $this->chunksize, $written);
			$hash = sha1($data);
			$size += fwrite($out, $data, $this->chunksize);
			$written += $size;
			$metadata[$name] = array('start' => $start, 'size' => $size, 'hash' => $hash);
			$part++;
		}

		$header = fopen('rados://'.$urn.'_header', 'w');
		$header_data = json_encode(array('parts' => count($metadata), 'metadata' => $metadata, 'size' => $written), JSON_PRETTY_PRINT);
		fwrite($header, $header_data);
	}

	/**
	 * @param $urn string the unified resource name used to identify the object
	 * @return void
	 * @throws IOException when something goes wrong
	 */
	public function deleteObject($urn) {
                $part = 1;
                $written = 0;

                $header = fopen('rados://'.$urn.'_header', 'r');
                $header_data = json_decode(stream_get_contents($header), true);

                while($part <= $header_data['parts']) {
                        $name = $urn . '_part' . $part;
			if (!unlink('rados://'.$name)) {
				throw new IOException('could not delete rados://'.$name);
			}
			$part++;
		}

		if (!unlink('rados://'.$urn.'_header')) {
			throw new IOException('could not delete rados://'.$urn.'_header');
		}

	}

	/**
	 * @param $urn string the unified resource name used to identify the object
	 * @return resource stream with the read data
	 * @throws IOException when something goes wrong
	 * @throws NotFoundException when object does not exist
	 */
	public function readObject($urn) {
		$part = 1;
		$written = 0;

		$header = fopen('rados://'.$urn.'_header', 'r');
		$header_data = json_decode(stream_get_contents($header), true);
		$tmpstream = fopen('php://temp', 'w+');

		while($part <= $header_data['parts']) {
			$name = $urn . '_part' . $part;
			$data = null;
			$stream = fopen('rados://'.$name, 'r');
			if (!$stream) {
				throw new NotFoundException('rados://'.$name.' not found');
			}
			$data = stream_get_contents($stream, $header_data['metadata'][$name]['size']);
			$hash = sha1($data);
			if($header_data['metadata'][$name]['hash'] == $hash) {
				$written += fwrite($tmpstream, $data);
			} else {
				throw new IOException($name.' hash: '.$hash.'does not match expected hash: '.$header_data['metadata'][$name]['hash']);
			}
			$part++;
		}

		rewind($tmpstream);
		return $tmpstream;
	}
}
