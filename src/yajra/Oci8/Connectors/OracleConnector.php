<?php namespace yajra\Oci8\Connectors;

use Illuminate\Database\Connectors;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;
use yajra\Pdo\Oci8;

class OracleConnector extends Connector implements ConnectorInterface {

	/**
	 * The default PDO connection options.
	 *
	 * @var array
	 */
	protected $options = [
		PDO::ATTR_CASE         => PDO::CASE_LOWER,
		PDO::ATTR_ERRMODE      => PDO::ERRMODE_EXCEPTION,
		PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
	];

	/**
	 * Create a new PDO connection.
	 *
	 * @param  string $tns
	 * @param  array $config
	 * @param  array $options
	 * @return PDO
	 */
	public function createConnection($tns, array $config, array $options)
	{
		// add fallback in case driver is not set, will use pdo instead
		if ( ! in_array($config['driver'], ['oci8', 'pdo-via-oci8', 'oracle']))
		{
			return parent::createConnection($tns, $config, $options);
		}

		$config = $this->setCharset($config);
		$options['charset'] = $config['charset'];

		return new Oci8($tns, $config['username'], $config['password'], $options);
	}

	/**
	 * Establish a database connection.
	 *
	 * @param array $config
	 * @return PDO
	 */
	public function connect(array $config)
	{
		$tns = $this->getDsn($config);

		$options = $this->getOptions($config);

		$connection = $this->createConnection($tns, $config, $options);

		// Like Postgres, Oracle allows the concept of "schema"
		$this->setSchema($config, $connection);

		return $connection;
	}

	/**
	 * Create a DSN string from a configuration.
	 *
	 * @param  array $config
	 * @return string
	 */
	protected function getDsn(array $config)
	{
		if ( ! empty($config['tns']))
		{
			return $config['tns'];
		}

		// parse configuration
		$config = $this->parseConfig($config);

		// check multiple connections/host, comma delimiter
		$config = $this->checkMultipleHostDsn($config);

		// return generated tns
		return $config['tns'];
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function checkMultipleHostDsn(array $config)
	{
		$host = is_array($config['host']) ? $config['host'] : explode(',', $config['host']);

		if (count($host) > 1)
		{
			$address = "";
			for ($i = 0; $i < count($host); $i++)
			{
				$address .= '(ADDRESS = (PROTOCOL = ' . $config["protocol"] . ')(HOST = ' . trim($host[$i]) . ')(PORT = ' . $config['port'] . '))';
			}

			// create a tns with multiple address connection
			$config['tns'] = "(DESCRIPTION = {$address} (LOAD_BALANCE = yes) (FAILOVER = on) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = {$config['database']})))";
		}

		return $config;
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function parseConfig(array $config)
	{
		$config = $this->setHost($config);
		$config = $this->setPort($config);
		$config = $this->setProtocol($config);
		$config = $this->setServiceId($config);
		$config = $this->setTNS($config);
		$config = $this->setCharset($config);

		return $config;
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function setHost(array $config)
	{
		$config['host'] = isset($config['host']) ? $config['host'] : $config['hostname'];

		return $config;
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function setPort(array $config)
	{
		$config['port'] = isset($config['port']) ? $config['port'] : '1521';

		return $config;
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function setProtocol(array $config)
	{
		$config['protocol'] = isset($config['protocol']) ? $config['protocol'] : 'TCP';

		return $config;
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function setServiceId(array $config)
	{
		$config['service'] = empty($config['service_name'])
			? $service_param = 'SID = ' . $config['database']
			: $service_param = 'SERVICE_NAME = ' . $config['service_name'];

		return $config;
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function setTNS(array $config)
	{
		$config['tns'] = "(DESCRIPTION = (ADDRESS = (PROTOCOL = {$config['protocol']})(HOST = {$config['host']})(PORT = {$config['port']})) (CONNECT_DATA =({$config['service']})))";

		return $config;
	}

	/**
	 * @param array $config
	 * @param $connection
	 */
	private function setSchema(array $config, $connection)
	{
		if (isset($config['schema']))
		{
			$schema = $config['schema'];
			$connection->setSchema($schema);
		}
	}

	/**
	 * @param array $config
	 * @return array
	 */
	private function setCharset(array $config)
	{
		if ( ! isset($config['charset']))
		{
			$config['charset'] = 'AL32UTF8';
		}

		return $config;
	}

}
