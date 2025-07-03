from sqlalchemy import text
import sqlalchemy as sa
import pytest
import time

def test_success_when_worker_is_up(sess):
    """net.check_worker_is_up should not return anything when the worker is running"""

    (result,) = sess.execute(text("""
        select net.wait_until_running();
        select net.check_worker_is_up();
    """)).fetchone()
    assert result is not None
    assert result == ''


def test_worker_will_process_queue_when_up(sess):
    """when pg background worker is down and requests arrive, it will process them once it wakes up"""

    # check worker up
    (up,) = sess.execute(text("""
        select is_worker_up();
    """)).fetchone()
    assert up is not None
    assert up == True

    # restart it
    (restarted,) = sess.execute(text("""
        select public.kill_worker();
    """)).fetchone()
    assert restarted is not None
    assert restarted == True

    time.sleep(0.1)

    # check worker down
    (up,) = sess.execute(text("""
        select is_worker_up();
    """)).fetchone()
    assert up is not None
    assert up == False

    sess.execute(text(
        """
        select net.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,10);
    """
    )).fetchone()

    sess.commit()

    # check requests where enqueued
    (count,) = sess.execute(text(
    """
        select count(*) from net.http_request_queue;
    """
    )).fetchone()

    assert count == 10

    # check worker is still down
    (up,) = sess.execute(text("""
        select is_worker_up();
    """)).fetchone()
    assert up is not None
    assert up == False

    sess.commit()

    # wait until up
    time.sleep(2.1)

    # check worker up
    (up,) = sess.execute(text("""
        select is_worker_up();
    """)).fetchone()
    assert up is not None
    assert up == True

    # wait until new requests are done
    time.sleep(1.1)

    (count,) = sess.execute(text(
    """
        select count(*) from net.http_request_queue;
    """
    )).fetchone()

    assert count == 0

    (status_code,count) = sess.execute(text(
    """
        select status_code, count(*) from net._http_response group by status_code;
    """
    )).fetchone()

    assert status_code == 200
    assert count == 10


def test_can_delete_rows_while_processing_queue(sess):
    """user can delete the queue rows while the worker is processing them"""

    # commit to avoid "cannot run inside a transaction block" error, see https://stackoverflow.com/a/75757326/4692662
    sess.execute(text("COMMIT"))
    sess.execute(text("alter system set pg_net.batch_size to '1';"))
    sess.execute(text("select net.worker_restart();"))
    sess.execute(text("select net.wait_until_running();"))

    sess.execute(text(
        """
        select net.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,10);
    """
    ))

    sess.commit()

    # leave time for some processing
    time.sleep(0.1)

    (count,) = sess.execute(text(
        """
        WITH deleted AS (DELETE FROM net.http_request_queue RETURNING *) SELECT count(*) FROM deleted;
    """
    )).fetchone()
    assert count > 1

    sess.commit()

    # commit to avoid "cannot run inside a transaction block" error, see https://stackoverflow.com/a/75757326/4692662
    sess.execute(text("COMMIT"))
    sess.execute(text("alter system reset pg_net.batch_size"))
    sess.execute(text("select net.worker_restart()"))
    sess.execute(text("select net.wait_until_running()"))


def test_truncate_wait_while_processing_queue(sess):
    """a truncate will wait until the worker is done processing all requests"""

    # commit to avoid "cannot run inside a transaction block" error, see https://stackoverflow.com/a/75757326/4692662
    sess.execute(text("COMMIT"))
    sess.execute(text("alter system set pg_net.batch_size to '1';"))
    sess.execute(text("select net.worker_restart();"))
    sess.execute(text("select net.wait_until_running();"))

    sess.execute(text(
        """
        select net.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,5);
    """
    ))

    sess.commit()

    sess.execute(text(
        """
        truncate net.http_request_queue;
    """
    ))

    sess.commit()

    (count,) = sess.execute(text(
        """
        select count(*) from net._http_response;
    """
    )).fetchone()
    assert count == 5

    sess.commit()

    # commit to avoid "cannot run inside a transaction block" error, see https://stackoverflow.com/a/75757326/4692662
    sess.execute(text("COMMIT"))
    sess.execute(text("alter system reset pg_net.batch_size"))
    sess.execute(text("select net.worker_restart()"))
    sess.execute(text("select net.wait_until_running()"))


def test_no_failure_on_drop_extension(sess):
    """while waiting for a slow request, a drop extension should wait and not crash the worker"""

    (request_id,) = sess.execute(text("""
        select net.http_get(url := 'http://localhost:8080/pathological?status=200&delay=2');
    """)).fetchone()
    assert request_id == 1

    sess.commit()

    # wait until processing
    time.sleep(1)

    sess.execute(text("""
        drop extension pg_net cascade;
    """))

    sess.commit()

    # wait until request is finished
    time.sleep(3)

    (up,) = sess.execute(text("""
        select is_worker_up();
    """)).fetchone()
    assert up is not None
    assert up == True


def test_worker_will_keep_processing_queue_when_restarted(sess):
    """when the background worker is restarted while working, it will pick up the remaining requests"""

    # commit to avoid "cannot run inside a transaction block" error, see https://stackoverflow.com/a/75757326/4692662
    sess.execute(text("COMMIT"))
    sess.execute(text("alter system set pg_net.batch_size to '1';"))
    sess.execute(text("select net.worker_restart();"))
    sess.execute(text("select net.wait_until_running();"))

    sess.execute(text(
        """
        select net.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,5);
    """
    ))

    sess.commit()

    # one restart will likely keep the worker awake since the signal could still be on, so do two restarts
    sess.execute(text(
        """
        select net.worker_restart();
        select net.wait_until_running();
    """
    ))

    time.sleep(0.1)

    sess.execute(text(
        """
        select net.worker_restart();
        select net.wait_until_running();
    """
    ))

    time.sleep(0.1)

    (status_code,count) = sess.execute(text(
    """
        select status_code, count(*) from net._http_response group by status_code;
    """
    )).fetchone()

    # at most 2 requests should have finished by now because of the low batch_size
    assert count <= 2
    assert count > 0 # at least 1 request should be finished
    assert status_code == 200

    # if we sleep for 4 seconds the whole 5 requests should be finished
    time.sleep(4)

    (status_code,count) = sess.execute(text(
    """
        select status_code, count(*) from net._http_response group by status_code;
    """
    )).fetchone()

    assert status_code == 200
    assert count == 5

    # commit to avoid "cannot run inside a transaction block" error, see https://stackoverflow.com/a/75757326/4692662
    sess.execute(text("COMMIT"))
    sess.execute(text("alter system reset pg_net.batch_size"))
    sess.execute(text("select net.worker_restart()"))
    sess.execute(text("select net.wait_until_running()"))


def test_new_requests_get_attended_asap(sess):
    """new requests get attended as soon as possible"""

    sess.execute(text("select net.wait_until_running();"))

    sess.execute(text(
        """
        select net.http_get('http://localhost:8080/pathological?status=200') from generate_series(1,10);
    """
    ))

    sess.commit()

    # less than a second
    time.sleep(0.1)

    (status_code,count) = sess.execute(text(
    """
        select status_code, count(*) from net._http_response group by status_code;
    """
    )).fetchone()

    assert status_code == 200
    assert count == 10
