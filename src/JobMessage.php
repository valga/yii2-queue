<?php

namespace yii\queue;

use yii\base\BaseObject;

class JobMessage extends BaseObject
{
    /** @var string */
    public $message;

    /** @var string */
    public $identity;

    /** @var int */
    public $ttr;

    /** @var int */
    public $delay;

    /**
     * @inheritDoc
     */
    public function __construct($config = [])
    {
        parent::__construct($config);
    }

    /**
     * @return static
     */
    public static function createFrom(JobInterface $job, Queue $queue, $delay = 0)
    {
        $message = $queue->serializer->serialize($job);
        $config = [
            'message' => $message,
            'identity' => $job instanceof JobIdentityInterface
                ? $job->getJobIdentity()
                : get_class($job) . ':' . sha1($message),
            'ttr' => $job instanceof RetryableJobInterface
                ? $job->getTtr()
                : $queue->ttr,
            'delay' => $delay,
        ];

        return new static($config);
    }
}
