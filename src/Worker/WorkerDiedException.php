<?php

declare(strict_types=1);

namespace App\Worker;

use RuntimeException;

/**
 * A worker process was gone at the moment we tried to hand it a job.
 *
 * Separate from a generic write failure because the caller has something
 * specific to do about it: the job it was trying to dispatch is still its
 * responsibility and has to go back to the queue - see
 * JobDispatcher::dispatch().
 */
final class WorkerDiedException extends RuntimeException
{
}
