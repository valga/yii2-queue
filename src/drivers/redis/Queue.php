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

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        $this->redis = Instance::ensure($this->redis, Connection::class);
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
            "$this->channel.attempts",
            "$this->channel.messages",
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
        $this->redis->eval(Scripts::CLEAR, 0, "$this->channel.*");
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
            "$this->channel.messages",
            "$this->channel.delayed",
            "$this->channel.reserved",
            "$this->channel.waiting",
            "$this->channel.attempts",
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
        $this->moveExpired("$this->channel.delayed");
        $this->moveExpired("$this->channel.reserved");

        // Find a new waiting message
        $result = $this->redis->eval(
            Scripts::RESERVE,
            4,
            "$this->channel.waiting",
            "$this->channel.messages",
            "$this->channel.reserved",
            "$this->channel.attempts",
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
            "$this->channel.waiting",
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
            "$this->channel.reserved",
            "$this->channel.attempts",
            "$this->channel.messages",
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
            "$this->channel.message_id",
            "$this->channel.messages",
            "$this->channel.waiting",
            "$this->channel.delayed",
            "{$ttr};{$message}",
            $delay,
            time()
        );
    }
}
