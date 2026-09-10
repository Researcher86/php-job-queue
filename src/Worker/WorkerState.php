<?php

declare(strict_types=1);

namespace App\Worker;

/**
 * A worker's position in its own lifecycle. The transitions live in
 * Worker::TRANSITIONS, which is the only thing that moves a worker between
 * them.
 */
enum WorkerState
{
    case STARTING;
    case IDLE;
    case BUSY;
    case DRAINING;
    case STOPPING;
    case DEAD;
}
