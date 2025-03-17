<?php

declare(strict_types=1);

namespace  Keboola\SnowflakeQueryHistory;

use Keboola\Csv\CsvFile;
use Keboola\SnowflakeQueryHistory\RunCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Serializer\Encoder\JsonDecode;

class RunCommandTest extends TestCase
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


    public function testRun(): void
    {

        $this->filesystem->dumpFile(
            $this->path . '/config.json',
            json_encode($this->createConfiguration()),
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

        $this->assertEquals(0, $commandTester->getStatusCode(), $commandTester->getDisplay());

        // queries should be fetched
        $this->assertFileExists($this->path . '/out/tables/queries.csv');
        $this->assertFileExists($this->path . '/out/tables/queries.csv.manifest');

        $queriesOutputCsvFile = new CsvFile($this->path . '/out/tables/queries.csv');
        $queriesManifestOutputCsvFile = json_decode(
            file_get_contents($this->path . '/out/tables/queries.csv.manifest'),
            true,
        );
        $this->assertContains('QUERY_ID', $queriesManifestOutputCsvFile['columns']);
        $endTimeIndex = array_search('END_TIME', $queriesManifestOutputCsvFile['columns']);

        // manifest is created
        $this->assertFileExists($this->path . '/out/tables/queries.csv.manifest');

        $manifest = (new JsonDecode([JsonDecode::ASSOCIATIVE => true]))
            ->decode(file_get_contents($this->path . '/out/tables/queries.csv.manifest'), 'json');
        $this->assertEquals(['QUERY_ID'], $manifest['primary_key']);
        $this->assertTrue($manifest['incremental']);

        // most recent query end time should be stored in state
        $this->assertFileExists($this->path . '/out/state.json');

        $state = (new JsonDecode(true))->decode(file_get_contents($this->path . '/out/state.json'), 'json');
        $this->assertArrayHasKey('latestEndTime', $state);

        $endTimes = array_map(
            function ($row) use ($endTimeIndex) {
                return $row[$endTimeIndex];
            },
            iterator_to_array($queriesOutputCsvFile),
        );

        $this->assertEquals(reset($endTimes), $state['latestEndTime']);

        echo "\n" . $commandTester->getDisplay() . "\n";
    }

    /**
     * @return array<string, array<string, string>>
     */
    private function createConfiguration(): array
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
