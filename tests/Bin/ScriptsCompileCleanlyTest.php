<?php

declare(strict_types=1);

namespace PhpJobQueue\Tests\Bin;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The runnable scripts are outside src/, so nothing loads them in a test -
 * and a compile-time warning (bench.php once had a no-op `use` of a global
 * class) is printed at the top of every run without failing anything.
 * `php -l` reports those warnings, so it is enough to ask it.
 */
final class ScriptsCompileCleanlyTest extends TestCase
{
    /** @return iterable<string, array{string}> */
    public static function scripts(): iterable
    {
        foreach ([...glob(__DIR__ . '/../../bin/*.php') ?: [], ...glob(__DIR__ . '/../../examples/*.php') ?: []] as $path) {
            yield basename(dirname($path)) . '/' . basename($path) => [$path];
        }
    }

    #[DataProvider('scripts')]
    public function testCompilesWithoutWarnings(string $path): void
    {
        exec(sprintf('%s -l %s 2>&1', escapeshellarg(PHP_BINARY), escapeshellarg($path)), $output, $exitCode);

        $this->assertSame(0, $exitCode);
        $this->assertSame(['No syntax errors detected in ' . $path], array_values(array_filter($output)));
    }
}
