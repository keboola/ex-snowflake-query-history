<?php

namespace  Keboola\SnowflakeQueryHistory;

class RunCommandErrorsTest extends \PHPUnit\Framework\TestCase
{

    private $path = '/tmp/application-test';

    /**
     * @var \Symfony\Component\Filesystem\Filesystem
     */
    private $filesystem;

    protected function setUp()
    {
        $this->filesystem = new \Symfony\Component\Filesystem\Filesystem();
        $this->filesystem->remove($this->path);
        $this->filesystem->mkdir($this->path);
        parent::setUp();
    }


    public function testEmptyConfigRun()
    {
        $this->filesystem->dumpFile(
            $this->path . '/config.json',
            json_encode(array([

            ]))
        );

        $this->filesystem->dumpFile(
            $this->path . '/in/state.json',
            '{}'
        );

        $command = new \Keboola\SnowflakeQueryHistory\RunCommand();
        $commandTester = new \Symfony\Component\Console\Tester\CommandTester($command);
        $commandTester->execute([
            'data directory' => $this->path,
        ]);

        $this->assertEquals(1, $commandTester->getStatusCode(), $commandTester->getDisplay());
        $this->assertEquals("The child node \"host\" at path \"parameters\" must be configured.\n", $commandTester->getDisplay());
    }

    public function testInvalidSnflkCredentialsRun()
    {
        $this->filesystem->dumpFile(
            $this->path . '/config.json',
            json_encode([
                'parameters' => [
                    'host' => getenv('SNOWFLAKE_HOST'),
                    'database' => getenv('SNOWFLAKE_DATABASE'),
                    'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
                    'user' => getenv('SNOWFLAKE_USER'),
                    '#password' => '123456',
                ],
            ])
        );

        $this->filesystem->dumpFile(
            $this->path . '/in/state.json',
            '{}'
        );

        $command = new \Keboola\SnowflakeQueryHistory\RunCommand();
        $commandTester = new \Symfony\Component\Console\Tester\CommandTester($command);
        $commandTester->execute([
            'data directory' => $this->path,
        ]);

        $this->assertEquals(1, $commandTester->getStatusCode(), $commandTester->getDisplay());
        $this->assertStringStartsWith('Initializing Snowflake connection failed', $commandTester->getDisplay());
    }
}
