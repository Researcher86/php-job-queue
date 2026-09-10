<?php

declare(strict_types=1);

namespace App\Worker;

use App\Job\Job;
use App\Job\JobResult;

/**
 * What came back from a worker about one job.
 *
 * The result is nullable, and that is the entire reason this exists rather
 * than a bare JobResult: null means the worker never answered at all - its
 * socket reached EOF, so the process is gone. "The job failed" and "the
 * worker vanished" need different handling (a NACK counts against attempts
 * and may end in the DLQ; a crash puts the job straight back and replaces
 * the worker), and a JobResult has no way to say the second one.
 *
 * The job travels with it because by the time an answer arrives the pool
 * has moved on: the outcome is the only thing that still knows which job
 * it was about.
 */
final readonly class WorkerOutcome
{
    public function __construct(
        private Job $job,
        // null = the worker died holding this job.
        private ?JobResult $result,
    ) {}

    public function getJob(): Job
    {
        return $this->job;
    }

    public function getResult(): ?JobResult
    {
        return $this->result;
    }

    /** Whether the worker died rather than answering. */
    public function isCrash(): bool
    {
        return $this->result === null;
    }
}
