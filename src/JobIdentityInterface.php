<?php

namespace yii\queue;

interface JobIdentityInterface
{
    /**
     * @return string
     */
    public function getJobIdentity();
}
