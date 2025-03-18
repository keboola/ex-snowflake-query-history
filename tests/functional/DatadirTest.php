<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Override;
use PHPUnit\Framework\AssertionFailedError;
use Symfony\Component\Process\Process;

class DatadirTest extends DatadirTestCase
{
    public static function assertStringMatchesFormat(string $format, string $string, string $message = ''): void
    {
        $expectedOutput = explode("\n", trim($format));
        $actualOutput = explode("\n", trim($string));

        foreach ($expectedOutput as $key => $line) {
            echo $line . PHP_EOL;
            echo $actualOutput[$key] . PHP_EOL;
            self::assertTrue(preg_match('/'.$line.'/', $actualOutput[$key]) === 1);
        }
    }

    #[Override]
    public function assertDirectoryContentsSame(string $expected, string $actual): void
    {
        if (str_contains($expected, 'run-action')) {
            $this->checkRunActionManifest($actual, $expected);

            // Check queries executed in previous test "FetcherTest::testFetch"
            $this->checkRunActionQueries($actual);
        } else {
            parent::assertDirectoryContentsSame($expected, $actual);
        }
    }

    private function checkRunActionQueries(string $actual): void
    {
        $content = file_get_contents($actual . '/tables/queries.csv');
        self::assertTrue(
            preg_match('/"SELECT \'99[0-9]{4}\'"/', $content) === 1,
        );
    }

    private function checkRunActionManifest(string $expected, string $actual): void
    {
        $expected = realpath($expected . '/tables/queries.csv.manifest');
        $actual = realpath($actual . '/tables/queries.csv.manifest');

        $diffProcess = new Process([
            'diff',
            '--exclude=.gitkeep',
            '--ignore-all-space',
            '--recursive',
            $expected,
            $actual,
        ]);

        $diffProcess->run();
        if ($diffProcess->getExitCode() > 0) {
            throw new AssertionFailedError(sprintf(
                'Two directories are not the same:' . PHP_EOL .
                '%s' . PHP_EOL .
                '%s' . PHP_EOL .
                '%s' . PHP_EOL .
                '%s',
                $expected,
                $actual,
                $diffProcess->getOutput(),
                $diffProcess->getErrorOutput(),
            ));
        }
    }
}
