<?php

declare(strict_types=1);

use App\Job\Job;
use App\Job\JobPriority;
use App\Producer\JobFactory;
use App\Producer\Producer;
use App\Queue\PriorityQueue;
use App\Queue\StrictPriority;
use App\Queue\WeightedRoundRobin;
use App\Support\SystemClock;

require __DIR__ . '/../vendor/autoload.php';

/**
 * Two priority policies against the same load, side by side.
 *
 *   make example EXAMPLE=priority-jobs
 *
 * The load is what a busy system looks like: a steady stream of HIGH work,
 * one new HIGH job arriving for every job served, plus a NORMAL and a LOW
 * backlog of NORMAL and LOW work that arrived before the rush started.
 *
 * Under StrictPriority the backlog never moves. Not "moves late" - never.
 * No amount of waiting helps, because nothing in that policy ever gives it
 * a turn. Under WeightedRoundRobin the pattern repeats every nine jobs and
 * HIGH still gets five of them.
 *
 * This runs the queue alone, with no workers: the question is which job
 * comes out next, and workers would only add noise to it.
 */

$rounds = 20;

foreach ([
    'strict priority' => new StrictPriority(),
    'weighted round robin (5:3:1)' => new WeightedRoundRobin(),
] as $label => $selector) {
    $clock = new SystemClock();
    $queue = new PriorityQueue($clock, $selector);
    $producer = new Producer($queue, new JobFactory($clock));

    // A backlog of less urgent work, waiting since before the rush.
    for ($i = 0; $i < 8; $i++) {
        $producer->dispatch('normal-report', priority: JobPriority::NORMAL);
        $producer->dispatch('low-cleanup', priority: JobPriority::LOW);
    }

    $served = [];
    for ($i = 0; $i < $rounds; $i++) {
        // The urgent work never lets up.
        $producer->dispatch('high-alert', priority: JobPriority::HIGH);

        $served[] = shortName($queue->pop());
    }

    printf("%-30s %s\n", $label, implode(' ', $served));
    printf("%-30s low-cleanup %s\n\n", '', in_array('L', $served, true) ? 'ran' : 'NEVER RAN');
}

/** One letter per job, so a run of twenty reads as a pattern. */
function shortName(?Job $job): string
{
    return $job === null ? '-' : $job->getPriority()->name[0];
}
