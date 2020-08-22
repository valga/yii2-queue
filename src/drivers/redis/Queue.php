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
                    if ($this->handleMessage($id, $message, $ttr, $attempt)) {
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
            $id
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
            5,
            $this->messagesKey,
            $this->delayedKey,
            $this->reservedKey,
            $this->waitingKey,
            $this->attemptsKey,
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
            3,
            $this->reservedKey,
            $this->attemptsKey,
            $this->messagesKey,
            $id
        );
    }

    /**
     * @inheritdoc
     */
    protected function pushMessage($message, $ttr, $delay, $priority)
    {
        if ($priority !== null) {
            throw new NotSupportedException('Job priority is not supported in the driver.');
        }

        return $this->redis->eval(
            Scripts::PUSH,
            4,
            $this->messageIdKey,
            $this->messagesKey,
            $this->waitingKey,
            $this->delayedKey,
            "{$ttr};{$message}",
            $delay,
            time()
        );
    }
}
