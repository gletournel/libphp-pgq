<?php
require_once __DIR__ . '/PGQConsumer.php';

define("PGQ_TRIGGER_NAME", "ins_to_queue");
define("PGQ_LAST_BATCH", "pgq_last_batch");

/**
 * PGQRemoteConsumer is a PGQConsumer to use when you need to process
 * the events at a remote site. It ensures a single batch won't get
 * processed more than once.
 *
 * PGQRemoteConsumer will consume events produced with
 * pgq.logutriga(), so a trigger as to be installed at the source
 * table.
 *
 * To ensure no batch is processed more than once, PGQRemoteConsumer
 * uses a table at destination side to store last processed batch
 * id.
 *
 * PGQRemoteConsumer will check if the trigger PGQ_TRIGGER_NAME is
 * installed, and will use PGQ_LAST_BATCH as the table name where to
 * find our last batch id.  The logs are explicit with respect to how
 * to define the trigger and to create the table.
 *
 * If the table name you give the constructor is NULL, the trigger will not
 * be checked at all. That allows you to use a non-SKIP trigger or have more
 * than one table producing events into the queue.
 *
 * When implementing a PGQConsumer, you should define following methods:
 *  - config()
 *  - process_event($event)  --- $event is a PGQEvent
 *
 * See PGQConsumer for more details.
 */
abstract class PGQRemoteConsumer extends PGQConsumer
{
    /**
     * Table name where to install pgq.logutriga trigger
     *
     * @var string
     */
    protected $table;

    /**
     * @var string
     */
    protected $dst_constr;

    /**
     * @var resource
     */
    protected $pg_dst_con;

    /**
     * @param string $cname
     * @param string $qname
     * @param string $table
     * @param int    $argc
     * @param array  $argv
     * @param string $src_constr
     * @param string $dst_constr
     */
    public function __construct($cname, $qname, $table, $argc, $argv, $src_constr, $dst_constr)
    {
        $this->table = $table;
        $this->dst_constr = $dst_constr;
        $this->last_batch_id = null;

        parent::__construct($cname, $qname, $argc, $argv, $src_constr);
    }

    /**
     * Manage batches of events, and call process_event() for each of them.
     *
     * At COMMIT time, we store current batch_id as the last processed
     * on destination database (while in the processing transaction),
     * as to be able to avoid re-processing it later.
     *
     * Manage source and destination database transactions.
     *
     * @param int $batch_id
     *
     * @return PGQEvent[]
     */
    public function preprocess_batch($batch_id)
    {
        $events = parent::preprocess_batch($batch_id);

        if ($events === false) {
            $this->log->debug("PGQRemoteConsumer.preprocess_batch got no events");
            return false;
        }
        pg_query($this->pg_dst_con, "BEGIN;");

        // WARNING: DO NOT USE switch() HERE
        $batch_done = $this->is_batch_done($batch_id);

        if ($batch_done === null) {
            // error getting last batch information, don't process_batch
            return false;
        } elseif ($batch_done === true) {
            // both connections are finished at this point
            return false;
        } elseif ($batch_done === false) {
            return $events;
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function postprocess_batch($batch_id, $abort_batch)
    {
        if ($abort_batch) {
            $this->rollback();
            return false;
        }

        if ($this->set_batch_done($batch_id)) {
            parent::postprocess_batch($batch_id);
            pg_query($this->pg_dst_con, "COMMIT;");
            return true;
        }

        // $this->rollback() has already been called when set_batch_done()
        // failed.
        return false;
    }

    /**
     * Get last processed batch_id
     *
     * @param int $batch_id
     *
     * @return bool
     */
    public function is_batch_done($batch_id)
    {
        $sql = sprintf(
            "SELECT batch_id FROM %s " .
            "WHERE qname = '%s' AND consumer_id = '%s'",
            PGQ_LAST_BATCH,
            pg_escape_string($this->qname),
            pg_escape_string($this->cname)
        );

        $this->log->debug($sql);
        if (($r = pg_query($this->pg_dst_con, $sql)) === false) {
            $this->log->warning("Could not retreive last processed batch id from dst database");
            $this->rollback();
            return null;
        }

        $this->last_batch_id = null;

        if (pg_num_rows($r) > 0) {
            $this->last_batch_id = pg_fetch_result($r, 0, 0);
        }
        $this->log->debug(
            "PGQRemoteConsumer.is_batch_done last_batch_id=%d batch_id=%d",
            $this->last_batch_id,
            $batch_id
        );

        if ($this->last_batch_id == null) {
            $this->log->warning("No last processed batch id");
            return false;
        } elseif ($batch_id <= $this->last_batch_id) {
            /**
             * batch already processed. As batch_id is a bigserial (see
             * pgq.next_batch SQL code), no wraparound risk here.
             */
            $this->log->verbose("Skipping batch %d, already processed (<= %d)", $batch_id, $this->last_batch_id);
            $this->finish_batch($batch_id);
            pg_query($this->pg_src_con, "COMMIT;");
            pg_query($this->pg_dst_con, "ROLLBACK;");

            return true;
        }
        $this->log->debug("PGQRemoteConsumer.is_batch_done === False");
        return false;
    }

    /**
     * last batch processed gets to be stored at remote database.
     *
     * @param int $batch_id
     *
     * @return bool
     */
    public function set_batch_done($batch_id)
    {
        if ($this->last_batch_id === null) {
            $sql = sprintf(
                "INSERT INTO pgq_last_batch(qname, consumer_id, batch_id) " .
                "VALUES ('%s', '%s', %d)",
                pg_escape_string($this->qname),
                pg_escape_string($this->cname),
                (int)$batch_id
            );
        } else {
            $sql = sprintf(
                "UPDATE pgq_last_batch SET batch_id = %d " .
                "WHERE qname = '%s' AND consumer_id = '%s'",
                (int)$batch_id,
                pg_escape_string($this->qname),
                pg_escape_string($this->cname)
            );
        }

        $this->log->debug($sql);
        if (($r = pg_query($this->pg_dst_con, $sql)) === false) {
            $this->log->error(
                "Could not store last_batch_id (%d) into " .
                "destination database, ROLLBACK",
                $batch_id
            );
            $this->rollback();
            return false;
        }

        // don't cache last_batch_id, ask it at remote database each time
        $this->last_batch_id = null;
        return true;
    }

    /**
     * Install must take care of creating the trigger, unless $this->table is
     * set to NULL.
     *
     * But we don't create the PGQ_LAST_BATCH table, which can be shared be
     * several daemons.
     *
     * We don't uninstall the trigger here, this could lead to data loss
     * if not done properly. You're on your own.
     *
     * @return bool
     */
    public function install()
    {
        $ret = parent::install();
        if ($ret && $this->table != null) {
            $ret = $this->install_trigger();
        }
        return $ret;
    }

    /**
     * PGQRemoteConsumer must have:
     *  - a PGQ_LAST_BATCH table on $dst_constr to store last processed batch_id
     *  - a PGQ_TRIGGER_NAME trigger on source $table which produces events
     *
     * @return bool
     */
    public function check()
    {
        if (parent::check() === false) {
            return false;
        }

        if ($this->connect() === false) {
            return false;
        }

        $ret = $this->check_pgq_last_batch();

        if ($ret && $this->table != null) {
            $ret = $this->check_pgq_trigger();
        }
        $this->disconnect();

        return $ret;
    }

    /**
     * Check we have a PGQ_LAST_BATCH table in remote database.
     *
     * @return bool
     */
    public function check_pgq_last_batch()
    {
        $sql_ct = sprintf("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = '%s'", PGQ_LAST_BATCH);
        $this->log->verbose("PGQRemoteConsumer: %s", $sql_ct);

        $result = pg_query($this->pg_dst_con, $sql_ct);

        if ($result === false) {
            $this->log->fatal("Could not check if table exist '%s'", PGQ_LAST_BATCH);
            return false;
        }

        if (pg_num_rows($result) == 0) {
            $this->log->fatal("Table %s doesn't exist on database '%s'", PGQ_LAST_BATCH, $this->dst_constr);

            // Be nice with user
            $this->log->fatal(
                "Please issue CREATE TABLE %s " .
                "(qname text, consumer_id text, batch_id bigint, " .
                "PRIMARY KEY (qname, consumer_id));",
                PGQ_LAST_BATCH
            );
            return false;
        }
        return true;
    }

    /**
     * Return the SQL for creating the trigger.
     *
     * @return string
     */
    public function trigger_sql()
    {
        return sprintf(
            "CREATE TRIGGER %s " .
            "BEFORE INSERT ON %s " .
            "FOR EACH ROW EXECUTE PROCEDURE " .
            "pgq.logutriga('%s', 'SKIP')",
            PGQ_TRIGGER_NAME,
            pg_escape_string($this->table),
            pg_escape_string($this->qname)
        );
    }

    /**
     * Check PGQ_TRIGGER_NAME is installed on $this->table
     *
     * @return bool
     */
    public function check_pgq_trigger()
    {
        $sql_ct = "SELECT t.tgname, pg_catalog.pg_get_triggerdef(t.oid) "
            . "  FROM pg_catalog.pg_trigger t "
            . "       JOIN pg_class c ON t.tgrelid = c.oid ";

        if (strpos($this->table, ".") > 0) {
            // table name with schema
            $tmp = explode(".", $this->table);
            $schemaname = $tmp[0];
            $tablename = $tmp[1];

            $sql_ct = sprintf(
                "%s WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '%s') " .
                "AND c.relname = '%s' AND tgname = '%s'",
                $sql_ct,
                pg_escape_string($schemaname),
                pg_escape_string($tablename),
                pg_escape_string(PGQ_TRIGGER_NAME)
            );
        } else {
            // non qualified table name
            $sql_ct = sprintf(
                "%s WHERE c.relname = '%s' AND tgname = '%s'",
                $sql_ct,
                pg_escape_string($this->table),
                pg_escape_string(PGQ_TRIGGER_NAME)
            );
        }

        $this->log->verbose("PGQRemoteConsumer check: %s", $sql_ct);
        if (($r = pg_query($this->pg_src_con, $sql_ct)) === false) {
            $this->log->fatal("PGQRemoteConsumer check: SQL error ON '%s'", $sql_ct);
            return false;
        }

        $sql_tr = $this->trigger_sql();
        $trigger_exists = (pg_num_rows($r) == 1);

        if ($trigger_exists == 1) {
            $triggerdef = pg_fetch_result($r, 0, 1);
            if ($triggerdef != $sql_tr) {
                $this->log->fatal(
                    "PGQRemoteConsumer check: " .
                    "%s TRIGGER already exists on " .
                    "table %s but is not PGQRemoteConsumer's",
                    PGQ_TRIGGER_NAME,
                    $this->table
                );

                $this->log->fatal("trigger def is        '%s'", PGQ_TRIGGER_NAME, $triggerdef);
                $this->log->fatal("trigger def should be '%s'", PGQ_TRIGGER_NAME, $sql_tr);
                $trigger_exists = false;
            }
        }
        return $trigger_exists;
    }

    /**
     * Installs trigger at source site.
     *
     * @return bool
     */
    public function install_trigger()
    {
        if ($this->connect() === false) {
            return false;
        }

        if ($this->check_pgq_trigger()) {
            $this->log->error("PGQRemoteConsumer trigger already exists");
            return false;
        }

        $sql_tr = $this->trigger_sql();
        $this->log->verbose("PGQRemoteConsumer: %s", $sql_tr);

        if (pg_query($this->pg_src_con, $sql_tr) === false) {
            $this->log->fatal("Could not install pgq.logutriga trigger to '%s'", $this->table);
            return false;
        }
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function connect($force = false)
    {
        if ($this->connected && !$force) {
            $this->log->notice("connect called when connected is True");
            return;
        }

        if ($this->dst_constr != "") {
            $this->log->verbose("Opening pg_dst connexion '%s'.", $this->dst_constr);
            $this->pg_dst_con = pg_connect($this->dst_constr);

            if ($this->pg_dst_con === false) {
                $this->log->fatal("Could not open pg_dst connextion '%s'.", $this->dst_constr);
                $this->stop();
            }
        }
        parent::connect($force);
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        if (!$this->connected) {
            $this->log->notice("disconnect called when $this->connected is False");
            return;
        }

        if ($this->pg_dst_con != null && $this->pg_dst_con !== false) {
            $this->log->verbose("Closing pg_dst connection '%s'.", $this->dst_constr);
            pg_close($this->pg_dst_con);
            $this->pg_dst_con = null;
        }
        parent::disconnect();
    }

    /**
     * {@inheritdoc}
     */
    protected function rollback()
    {
        if ($this->pg_dst_con != null && $this->pg_dst_con !== false) {
            $this->log->notice("ROLLBACK pg_dst connection '%s'.", $this->dst_constr);
            pg_query($this->pg_dst_con, "ROLLBACK;");
        }
        parent::rollback();
    }
}
