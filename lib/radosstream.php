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

class RadosStream {
	const MODE_FILE = 0100000;

	protected $rados;
	protected $pool = 'owncloud';
	protected $ioctx;

	protected $pos = 0;
	protected $stat = [ 'psize' => 0 ];
	protected $writable = true;

	/**
	 * @param string $path
	 * @return string
	 */
	private function path2oid ($path) {
		return substr($path, 8); // 8 = strlen('rados://')
	}

	/**
	 * @throws IOException
	 */
	public function stream_close() {
		$res = \rados_ioctx_destroy($this->ioctx);
		if ($res < 0) {
			throw new IOException('Could not destroy io context after reading '.$this->stat['oid']);
		}
		$ret = \rados_shutdown($this->rados);
		if ($ret < 0) {
			throw new IOException('Could not shutdown rados resource');
		}
	}

	/**
	 * @return bool
	 */
	public function stream_eof() {
		return $this->pos >= $this->stat['psize'];
	}

	/**
	 * @param string $path
	 * @param string $mode
	 * @param int $options
	 * @param string $opened_path
	 * @return bool
	 * @throws IOException
	 */
	public function stream_open($path, $mode, $options, &$opened_path) {
		$ret = $this->rados = \rados_create();
		if (!$ret) {
			throw new IOException('Could not create rados resource');
		}

		$configPath = '/etc/ceph/ceph.conf';
		$ret = \rados_conf_read_file($this->rados, $configPath);
		if (!$ret) {
			throw new IOException("Could not read config from $configPath");
		}

		$ret = \rados_connect($this->rados);
		if (!$ret) {
			throw new IOException('Could not connect to rados');
		}

		$id = \rados_pool_lookup($this->rados, $this->pool);

		//TODO make autocreation configurable
		if ($id < 0) {
			$ret = \rados_pool_create($this->rados, $this->pool);
			if ($ret < 0) {
				throw new IOException("Could not create pool '$this->pool'");
			}
		}
		$this->ioctx = \rados_ioctx_create($this->rados, $this->pool);
		if (!$this->ioctx) {
			throw new IOException("Could not create io context before reading object $path");
		}
		$oid = $this->path2oid($path);
		$this->stat = \rados_stat($this->ioctx, $oid);
		switch ($mode[0]) {
			case 'r':
				if (!$this->stat) {
					return false;
				}
				$this->writable = isset($mode[1]) && $mode[1] == '+';
				break;
			case 'a':
				if ($this->stat && $this->stat['psize']) {
					$this->pos = $this->stat['psize'];
				}
				break;
			case 'x':
				if (!$this->stat) {
					return false;
				}
				break;
			case 'w':
			case 'c':
				if (!$this->stat) {
					$this->stat = ['psize' => 0, 'oid' => $oid];
				}
				break;
			default:
				return false;
		}
		return true;
	}

	/**
	 * @param int $count
	 * @return string
	 */
	public function stream_read($count) {
		if ($count > ($this->stat['psize'] - $this->pos)) {
			$count = $this->stat['psize'] - $this->pos;
		}
		if ($this->stream_eof()) {
			return '';
		}
		$buf = \rados_read($this->ioctx, $this->stat['oid'], $count, $this->pos);
		//TODO check return type?
		$this->pos += $count;
		return $buf;
	}

	/**
	 * @param int $offset
	 * @param int $whence
	 * @return bool
	 */
	public function stream_seek($offset, $whence = SEEK_SET) {
		$len = $this->stat['psize'];
		switch ($whence) {
			case SEEK_SET:
				if ($offset <= $len) {
					$this->pos = $offset;
					return true;
				}
				break;
			case SEEK_CUR:
				if ($this->pos + $offset <= $len) {
					$this->pos += $offset;
					return true;
				}
				break;
			case SEEK_END:
				if ($len + $offset <= $len) {
					$this->pos = $len + $offset;
					return true;
				}
				break;
		}
		return false;
	}

	/**
	 * @return array
	 */
	public function stream_stat() {
		return $this->url_stat('', 0);
	}

	/**
	 * @return int
	 */
	public function stream_tell() {
		return $this->pos;
	}

	/**
	 * @param string $data
	 * @return int
	 * @throws RadosException
	 */
	public function stream_write($data) {
		if (!$this->writable) {
			return 0;
		}
		if (!$this->stat) {
			throw new IOException('stat unknown');
		}
		$size = strlen($data);

		$ret = \rados_write($this->ioctx, $this->stat['oid'], $data, $this->pos);
		if (!$ret) {
			throw new IOException("Could not write to '".$this->stat['oid']."'");
		}
		$this->pos += $size;
		return $size;
	}

	/**
	 * @param string $path
	 * @return bool
	 * @throws RadosException
	 */
	public function unlink($path) {
		$openedPath = null;

		if (!$this->stream_open($path,'r', null, $openedPath)) {
			return false;
		}
		$ret = \rados_remove($this->ioctx, $this->stat['oid']);
		if (!$ret) {
			throw new IOException("Could not delete '".$this->stat['oid']."'");
		}
		$this->stream_close();

		return true;
	}

	/**
	 * @param string $path
	 * @param int $flags
	 * @return array
	 */
	public function url_stat($path, $flags) {
			$size = $this->stat['psize'];
			$time = $this->stat['ptime'];
			$data = array(
				'dev' => 0,
				'ino' => 0,
				'mode' => self::MODE_FILE | 0777,
				'nlink' => 1,
				'uid' => 0,
				'gid' => 0,
				'rdev' => '',
				'size' => $size,
				'atime' => $time,
				'mtime' => $time,
				'ctime' => $time,
				'blksize' => -1,
				'blocks' => -1,
			);
			return array_values($data) + $data;
	}
}
