<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory\Config;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    /**
     * @return array<string, string>
     */
    public function getConnectionConfig(): array
    {
        return [
            'host' => $this->getStringValue(['parameters', 'host']),
            'user' => $this->getStringValue(['parameters', 'user']),
            'privateKey' => $this->getStringValue(['parameters', '#privateKey']),
            'warehouse' => $this->getStringValue(['parameters', 'warehouse']),
            'database' => $this->getStringValue(['parameters', 'database']),
        ];
    }

    public function getHost(): string
    {
        return $this->getStringValue(['parameters', 'host']);
    }
}
