<?php

declare(strict_types=1);

namespace Keboola\SnowflakeQueryHistory;

class InvalidRowsFilterTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @dataProvider filterRowsProvider
     * @param array $expectedRows
     * @param array $fetchedRows
     */
    public function testFilterRows(array $expectedRows, array $fetchedRows)
    {
        $this->assertEquals($expectedRows, Fetcher::filterRowsWithValidEndTime($fetchedRows));
    }

    public function filterRowsProvider(): array
    {
        return [
            [
                [
                    [
                        'STATUS' => 'SUCCESS',
                        'END_TIME' => '2019-01-05 13:21:19.361000'
                    ],
                    [
                        'STATUS' => 'SUCCESS',
                        'END_TIME' => '2019-01-05 13:21:18.468000'
                    ],
                ],
                [
                    [
                        'STATUS' => 'SUCCESS',
                        'END_TIME' => '2019-01-05 13:21:19.361000'
                    ],
                    [
                        'STATUS' => 'SUCCESS',
                        'END_TIME' => '2019-01-05 13:21:18.468000'
                    ],
                    [
                        'STATUS' => 'RUNNING',
                        'END_TIME' => '1970-01-01 00:00:00'
                    ],
                ],
            ]
        ];
    }
}
