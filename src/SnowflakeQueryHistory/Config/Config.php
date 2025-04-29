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
        $options = [
            'host' => $this->getStringValue(['parameters', 'host']),
            'user' => $this->getStringValue(['parameters', 'user']),
            'password' => $this->getStringValue(['parameters', '#password'], ''),
            'warehouse' => $this->getStringValue(['parameters', 'warehouse']),
            'database' => $this->getStringValue(['parameters', 'database']),
        ];

        if ($this->hasPrivateKey()) {
            $options['privateKey'] = $this->getStringValue(['parameters', '#privateKey']);
        }

        return $options;
    }

    public function getHost(): string
    {
        return $this->getStringValue(['parameters', 'host']);
    }

    private function hasPrivateKey(): bool
    {
        return !empty($this->getValue(['parameters', '#privateKey'], ''));
    }
}
