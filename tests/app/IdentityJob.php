<?php

namespace tests\app;

use yii\queue\JobIdentityInterface;
use yii\queue\JobInterface;
use yii\queue\Queue;

class IdentityJob implements JobInterface, JobIdentityInterface
{
    /** @var string */
    private $identity;

    /**
     * @param string $identity
     */
    public function __construct($identity)
    {
        $this->identity = $identity;
    }

    /**
     * @inheritDoc
     */
    public function getJobIdentity()
    {
        return get_class($this) . ':' . $this->identity;
    }

    /**
     * @inheritDoc
     */
    public function execute($queue)
    {
    }
}
