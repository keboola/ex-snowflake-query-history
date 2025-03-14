<?php

namespace Keboola\SnowflakeQueryHistory;

use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeQueryHistory\Config\Config;
use Keboola\SnowflakeQueryHistory\Config\ConfigDefinition;

class Component extends BaseComponent
{
    protected function run(): void
    {
        $connection = new Connection(
            $this->getConfig()->getConnectionConfig()
        );

        $connection->fetchAll('SELECT 1');
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
