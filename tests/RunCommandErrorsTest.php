<?php

declare(strict_types=1);

namespace  Keboola\SnowflakeQueryHistory;

use Keboola\SnowflakeQueryHistory\RunCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Filesystem\Filesystem;

class RunCommandErrorsTest extends TestCase
{
    private string $path = '/tmp/application-test';

    private Filesystem $filesystem;

    protected function setUp(): void
    {
        $this->filesystem = new Filesystem();
        $this->filesystem->remove($this->path);
        $this->filesystem->mkdir($this->path);
        parent::setUp();
    }


    public function testEmptyConfigRun(): void
    {
        $this->filesystem->dumpFile(
            $this->path . '/config.json',
            json_encode(
                [[

                ]],
            ),
        );

        $this->filesystem->dumpFile(
            $this->path . '/in/state.json',
            '{}',
        );

        $command = new RunCommand();
        $commandTester = new CommandTester($command);
        $commandTester->execute(
            [
            'data directory' => $this->path,
            ],
        );

        $this->assertEquals(1, $commandTester->getStatusCode(), $commandTester->getDisplay());
        $this->assertEquals(
            "The child node \"host\" at path \"parameters\" must be configured.\n",
            $commandTester->getDisplay(),
        );
    }

    public function testInvalidSnflkCredentialsRun(): void
    {
        $this->filesystem->dumpFile(
            $this->path . '/config.json',
            json_encode(
                [
                'parameters' => [
                    'host' => getenv('SNOWFLAKE_HOST'),
                    'database' => getenv('SNOWFLAKE_DATABASE'),
                    'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
                    'user' => getenv('SNOWFLAKE_USER'),
                    '#password' => '123456',
                ],
                ],
            ),
        );

        $this->filesystem->dumpFile(
            $this->path . '/in/state.json',
            '{}',
        );

        $command = new RunCommand();
        $commandTester = new CommandTester($command);
        $commandTester->execute(
            [
            'data directory' => $this->path,
            ],
        );

        $this->assertEquals(1, $commandTester->getStatusCode(), $commandTester->getDisplay());
        $this->assertStringStartsWith('Initializing Snowflake connection failed', $commandTester->getDisplay());
    }
}
