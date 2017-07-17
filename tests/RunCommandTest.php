<?php

class RunCommandTest extends \PHPUnit\Framework\TestCase
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


    public function testRun()
    {

        $this->filesystem->dumpFile(
            $this->path . '/config.json',
            json_encode($this->createConfiguration())
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

        $this->assertEquals(0, $commandTester->getStatusCode(), $commandTester->getDisplay());

        // queries should be fetched
        $this->assertFileExists($this->path . "/out/tables/queries.csv");

        $queriesOutputCsvFile = new \Keboola\Csv\CsvFile($this->path . "/out/tables/queries.csv");
        $this->assertContains('QUERY_ID', $queriesOutputCsvFile->getHeader());
        $endTimeIndex = array_search('END_TIME', $queriesOutputCsvFile->getHeader());

        // most recent query end time should be stored in state
        $this->assertFileExists($this->path . "/out/state.json");

        $state = (new \Symfony\Component\Serializer\Encoder\JsonDecode(true))->decode(file_get_contents($this->path . "/out/state.json"), 'json');
        $this->assertArrayHasKey('latestEndTime', $state);

        $endTimes = array_map(function($row) use ($endTimeIndex) {
            return $row[$endTimeIndex];
        }, iterator_to_array($queriesOutputCsvFile));
        array_shift($endTimes); // remove header

        $this->assertEquals(reset($endTimes), $state['latestEndTime']);

    }

    private function createConfiguration()
    {
        return [
            'parameters' => [
                'host' => getenv('SNOWFLAKE_HOST'),
                'database' => getenv('SNOWFLAKE_DATABASE'),
                'warehouse' => getenv('SNOWFLAKE_WAREHOUSE'),
                'user' => getenv('SNOWFLAKE_USER'),
                '#password' => getenv('SNOWFLAKE_PASSWORD'),
            ],
        ];
    }
}