<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace yii\queue\redis;

use yii\base\InvalidArgumentException;
use yii\base\NotSupportedException;
use yii\di\Instance;
use yii\queue\cli\Queue as CliQueue;
use yii\queue\JobMessage;
use yii\redis\Connection;

/**
 * Redis Queue.
 *
 * @author Roman Zhuravlev <zhuravljov@gmail.com>
 */
class Queue extends CliQueue
{
    /**
     * @var Connection|array|string
     */
    public $redis = 'redis';
    /**
     * @var string
     */
    public $channel = 'queue';
    /**
     * @var string command class name
     */
    public $commandClass = Command::class;

    /** @var string */
    private $reservedKey;

    /** @var string */
    private $attemptsKey;

    /** @var string */
    private $messagesKey;

    /** @var string */
    private $messageIdKey;

    /** @var string */
    private $waitingKey;

    /** @var string */
    private $delayedKey;

    /** @var string */
    private $wildcardKey;

    /** @var string */
    private $identityLockKey;

    /** @var string */
    private $identityIndexKey;

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->redis = Instance::ensure($this->redis, Connection::class);
        $this->attemptsKey = "$this->channel.attempts";
        $this->messagesKey = "$this->channel.messages";
        $this->wildcardKey = "$this->channel.*";
        $this->delayedKey = "$this->channel.delayed";
        $this->waitingKey = "$this->channel.waiting";
        $this->reservedKey = "$this->channel.reserved";
        $this->messageIdKey = "$this->channel.message_id";
        $this->identityLockKey = "$this->channel.job_id_lock";
        $this->identityIndexKey = "$this->channel.job_id_index";
    }

    /**
     * Listens queue and runs each job.
     *
     * @param bool $repeat whether to continue listening when queue is empty.
     * @param int $timeout number of seconds to wait for next message.
     * @return null|int exit code.
     * @internal for worker command only.
     * @since 2.0.2
     */
    public function run($repeat, $timeout = 0)
    {
        return $this->runWorker(function (callable $canContinue) use ($repeat, $timeout) {
            while ($canContinue()) {
                if (($payload = $this->reserve($timeout)) !== null) {
                    list($id, $message, $ttr, $attempt) = $payload;
                    $result = $this->handleMessage($id, $message, $ttr, $attempt);
                    if ($result instanceof JobMessage) {
                        $this->deleteAndPush($id, $result);
                    } elseif ($result) {
                        $this->delete($id);
                    }
                } elseif (!$repeat) {
                    break;
                }
            }
        });
    }

    /**
     * @inheritdoc
     */
    public function status($id)
    {
        if (!is_numeric($id) || $id <= 0) {
            throw new InvalidArgumentException("Unknown message ID: $id.");
        }

        return (int) $this->redis->eval(
            Scripts::STATUS,
            2,
            $this->attemptsKey,
            $this->messagesKey,
            $id,
            self::STATUS_WAITING,
            self::STATUS_RESERVED,
            self::STATUS_DONE
        );
    }

    /**
     * Clears the queue.
     *
     * @since 2.0.1
     */
    public function clear()
    {
        $this->redis->eval(Scripts::CLEAR, 0, $this->wildcardKey);
    }

    /**
     * Removes a job by ID.
     *
     * @param int $id of a job
     * @return bool
     * @since 2.0.1
     */
    public function remove($id)
    {
        return (bool) $this->redis->eval(
            Scripts::REMOVE,
            7,
            $this->messagesKey,
            $this->delayedKey,
            $this->reservedKey,
            $this->waitingKey,
            $this->attemptsKey,
            $this->identityIndexKey,
            $this->identityLockKey,
            $id
      );
    }

    /**
     * @param int $timeout timeout
     * @return array|null payload
     */
    protected function reserve($timeout)
    {
        // Moves delayed and reserved jobs into waiting list
        $this->moveExpired($this->delayedKey);
        $this->moveExpired($this->reservedKey);

        // Find a new waiting message
        $result = $this->redis->eval(
            Scripts::RESERVE,
            4,
            $this->waitingKey,
            $this->messagesKey,
            $this->reservedKey,
            $this->attemptsKey,
            time()
        );

        if (empty($result)) {
            if ($timeout) {
                usleep(10000);
            }
            return null;
        }

        return $result;
    }

    /**
     * @param string $from
     */
    protected function moveExpired($from)
    {
        $this->redis->eval(
            Scripts::MOVE_EXPIRED,
            2,
            $from,
            $this->waitingKey,
            time()
        );
    }

    /**
     * Deletes message by ID.
     *
     * @param int $id of a message
     */
    protected function delete($id)
    {
        $this->redis->eval(
            Scripts::DELETE,
            5,
            $this->reservedKey,
            $this->attemptsKey,
            $this->messagesKey,
            $this->identityIndexKey,
            $this->identityLockKey,
            $id
        );
    }

    /**
     * @param string $id
     * @return string
     */
    protected function deleteAndPush($id, JobMessage $job)
    {
        return $this->redis->eval(
            Scripts::DELETE_AND_PUSH,
            8,
            $this->reservedKey,
            $this->attemptsKey,
            $this->messagesKey,
            $this->identityIndexKey,
            $this->identityLockKey,
            $this->messageIdKey,
            $this->waitingKey,
            $this->delayedKey,
            $id,
            $job->identity,
            "{$job->ttr};{$job->message}",
            $job->delay,
            time()
        );
    }

    /**
     * @inheritdoc
     */
    protected function pushMessage($message, $ttr, $delay, $identity)
    {
        return $this->redis->eval(
            Scripts::PUSH,
            7,
            $this->messageIdKey,
            $this->messagesKey,
            $this->waitingKey,
            $this->delayedKey,
            $this->identityLockKey,
            $this->identityIndexKey,
            $this->reservedKey,
            "{$ttr};{$message}",
            $delay,
            time(),
            $identity
        );
    }
}
