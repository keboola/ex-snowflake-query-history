<?php

namespace Keboola\SnowflakeQueryHistory;

use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeQueryHistory\Config\Config;
use Keboola\SnowflakeQueryHistory\Config\ConfigDefinition;
use Psr\Log\LoggerInterface;

class Component extends BaseComponent
{
    private Connection $connection;

    private Fetcher $fetcher;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);

        $this->connection = new Connection(
            $this->getConfig()->getConnectionConfig()
        );

        $this->fetcher = new Fetcher($this->connection);
    }

    protected function run(): void
    {
        $this->connection->fetchAll(
            'SELECT 1'
        );
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = $this->config;

        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
