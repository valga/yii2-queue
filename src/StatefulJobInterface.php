<?php

namespace yii\queue;

interface StatefulJobInterface
{
    /** @return JobMessage|null */
    public function getNextStateJob(Queue $queue);
}
