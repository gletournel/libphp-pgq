<?php
require_once("pgq/PGQRemoteConsumer.php");

/**
 * PGQEventRemoteConsumer is a PGQRemoteConsumer which handles nested
 * transactions for event management, allowing the remote processing
 * to be commited or rollbacked at event level.
 */
abstract class PGQCoopConsumer extends PGQConsumer
{
    /**
     * Subconsumer name
     *
     * @var string
     */
    protected $sname;

    /**
     * @var int
     */
    protected $timeout = null;

    /**
     * @param string $sname Subconsumer name
     * @param string $cname
     * @param string $qname
     * @param int    $argc
     * @param array  $argv
     * @param string $src_constr
     */
    public function __construct($sname, $cname, $qname, $argc, $argv, $src_constr)
    {
        $this->sname = $sname;
        parent::__construct($cname, $qname, $argc, $argv, $src_constr);
    }

    /**
     * {@inheritdoc}
     */
    protected function register()
    {
        $sql = sprintf(
            "SELECT pgq_coop.register_subconsumer('%s', '%s', '%s');",
            pg_escape_string($this->qname),
            pg_escape_string($this->cname),
            pg_escape_string($this->sname)
        );

        $this->log->verbose("%s", $sql);
        $r = pg_query($this->pg_src_con, $sql);
        if ($r === false) {
            $this->log->warning(
                "Could not register subconsumer '%s' of '%s' to queue '%s'",
                $this->sname,
                $this->cname,
                $this->qname
            );
            return false;
        }

        $registered = pg_fetch_result($r, 0, 0);
        if ($registered == "1") {
            return true;
        } else {
            $this->log->fatal("Register SubConsumer failed (%d).", $registered);
            return false;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function unregister()
    {
        $sql = sprintf(
            "SELECT pgq_coop.unregister_subconsumer('%s', '%s', '%s', 0);",
            pg_escape_string($qname),
            pg_escape_string($cname),
            pg_escape_string($sname)
        );

        $this->log->verbose("%s", $sql);
        $r = pg_query($this->pg_src_con, $sql);
        if ($r === false) {
            $this->log->fatal(
                "Could not unregister subconsumer '%s' of '%s' to queue '%s'",
                $this->sname,
                $this->cname,
                $this->qname
            );
            return false;
        }

        $unregistered = pg_fetch_result($r, 0, 0);
        if ($unregistered == "1") {
            return true;
        } else {
            $this->log->fatal("Unregister SubConsumer failed (%d).", $unregistered);
            return false;
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function next_batch()
    {
        if ($this->timeout !== null) {
            $sql = sprintf(
                "SELECT pgq_coop.next_batch('%s', '%s', '%s', '%s')",
                pg_escape_string($this->qname),
                pg_escape_string($this->cname),
                pg_escape_string($this->sname),
                $this->timeout
            );
        } else {
            $sql = sprintf(
                "SELECT pgq_coop.next_batch('%s', '%s', '%s')",
                pg_escape_string($this->qname),
                pg_escape_string($this->cname),
                pg_escape_string($this->sname)
            );
        }

        $this->log->verbose("%s", $sql);
        if (($r = pg_query($this->pg_src_con, $sql)) === false) {
            $this->log->error("Could not get next batch");
            return false;
        }

        $batch_id = pg_fetch_result($r, 0, 0);
        $this->log->debug(
            "Get batch_id %s (isnull=%s)",
            $batch_id,
            ($batch_id === null ? "True" : "False")
        );
        return $batch_id;
    }

    /**
     * {@inheritdoc}
     */
    protected function finish_batch($batch_id)
    {
        $sql = sprintf("SELECT pgq_coop.finish_batch(%d);", (int)$batch_id);

        $this->log->verbose("%s", $sql);
        if (pg_query($this->pg_src_con, $sql) === false) {
            $this->log->error("Could not finish batch %d", (int)$batch_id);
            return false;
        }
        return true;
    }

    /**
     * {@inheritdoc}
     */
    protected function get_consumer_info()
    {
        return PGQ::get_consumer_info(
            $this->log,
            $this->pg_src_con,
            $this->qname,
            $this->cname . "." . $this->sname
        );
    }
}
