<?php

namespace tests\app;

use yii\queue\JobIdentityInterface;
use yii\queue\JobInterface;
use yii\queue\JobMessage;
use yii\queue\Queue;
use yii\queue\StatefulJobInterface;
use Yii;

class StatefulJob implements JobInterface, JobIdentityInterface, StatefulJobInterface
{
    /** @var int */
    private $iterationsLeft;

    /** @var int|null */
    private $failOnIteration;

    /** @var string */
    private $uid;

    /**
     * @param int $iterationsLeft
     */
    public function __construct($iterationsLeft, $failOnIteration = null)
    {
        $this->iterationsLeft = $iterationsLeft;
        $this->failOnIteration = $failOnIteration;
        $this->uid = uniqid();
    }

    /**
     * @inheritDoc
     */
    public function getJobIdentity()
    {
        return get_class($this) . ':' . $this->uid;
    }

    /**
     * @inheritDoc
     */
    public function execute($queue)
    {
        if ($this->failOnIteration === $this->iterationsLeft) {
            throw new \RuntimeException('Just as expected');
        }
        file_put_contents($this->getFileName(), $this->iterationsLeft, FILE_APPEND);
        $this->iterationsLeft--;
    }

    /**
     * @inheritDoc
     */
    public function getNextStateJob(Queue $queue)
    {
        if ($this->iterationsLeft <= 0) {
            return null;
        }

        $job = new static($this->iterationsLeft, $this->failOnIteration);
        $job->uid = $this->uid;

        return JobMessage::createFrom($job, $queue);
    }

    public function getFileName()
    {
        return Yii::getAlias("@runtime/job-{$this->uid}.lock");
    }
}
