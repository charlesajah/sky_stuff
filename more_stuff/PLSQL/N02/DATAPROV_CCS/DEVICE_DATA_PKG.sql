CREATE OR REPLACE PACKAGE device_data_pkg AS
PROCEDURE reset_boxo_sequence ;
PROCEDURE hub3_device_inserts ( p_prefix IN VARCHAR2 , p_orderNum IN NUMBER , p_mftNum IN VARCHAR2 , p_partNum IN VARCHAR2 , p_volume IN NUMBER ) ;
PROCEDURE skyQ_device_inserts ( p_prefix IN VARCHAR2 , p_volume IN NUMBER ) ;
PROCEDURE serial_device_inserts ( p_custOrder IN NUMBER , p_partNum IN VARCHAR2 , p_prefix IN VARCHAR2 , p_volume IN NUMBER ) ;
PROCEDURE non_serial_device_inserts ( p_custOrder IN NUMBER , p_partNum IN VARCHAR2 , p_volume IN NUMBER ) ;
PROCEDURE get_device_detail ( v_testName IN VARCHAR2 , v_rec_out OUT SYS_REFCURSOR ) ;
END device_data_pkg ;
/


CREATE OR REPLACE PACKAGE BODY device_data_pkg AS

PROCEDURE reset_boxo_sequence
IS
   l_value NUMBER ;
BEGIN
   -- Select the next value of the sequence
   execute immediate 'SELECT boxoSeq.NEXTVAL - 100000 FROM DUAL' INTO l_value ;
   -- Set a negative increment for the sequence, with value = the current value of the sequence
   execute immediate 'ALTER SEQUENCE boxoSeq INCREMENT BY -' || l_value || ' MINVALUE 100000' ;
   -- Select once FROM the sequence, to take its current value back to 0
   execute immediate 'SELECT boxoSeq.NEXTVAL FROM DUAL' INTO l_value ;
   -- Set the increment back to 1
   execute immediate 'ALTER SEQUENCE boxoSeq INCREMENT BY 1 MINVALUE 100000';
END reset_boxo_sequence ;

PROCEDURE hub3_device_inserts ( p_prefix IN VARCHAR2 , p_orderNum IN NUMBER , p_mftNum IN VARCHAR2 , p_partNum IN VARCHAR2 , p_volume IN NUMBER )
IS
BEGIN
   INSERT INTO hub3_devices ( pk_value , serial_no , order_no , mft_no , part_no , seqNo )
   SELECT SYS_GUID() AS pk_value
        , p_prefix || TO_CHAR ( boxoSeq.NEXTVAL ) AS serial_no
        , p_orderNum AS order_no
        , p_mftNum AS mft_no
        , p_partNum AS part_no
        , 0 AS seqNo
     FROM DUAL
  CONNECT BY LEVEL <= p_volume
   ;
   COMMIT ;
END hub3_device_inserts ;

PROCEDURE skyQ_device_inserts ( p_prefix IN VARCHAR2 , p_volume IN NUMBER )
IS
BEGIN
   INSERT INTO skyq_devices ( pk_value , serial_no , seqNo , test_alloc )
   SELECT SYS_GUID() AS pk_value
        , p_prefix || TO_CHAR ( skyQseq.NEXTVAL ) AS serial_no
        , 0 AS seqNo
        , 'SKYQ_DEVICES' AS test_alloc
     FROM DUAL
  CONNECT BY LEVEL <= p_volume
   ;
   COMMIT ;
END skyQ_device_inserts ;

PROCEDURE serial_device_inserts ( p_custOrder IN NUMBER , p_partNum IN VARCHAR2 , p_prefix IN VARCHAR2 , p_volume IN NUMBER )
IS
begin
   INSERT INTO serial_devices ( pk_value , custOrder , part_no , serial_no , seqNo , test_alloc )
   SELECT SYS_GUID() AS pk_value
         , p_custorder AS custOrder
         , p_partnum AS part_no
         , p_prefix || TO_CHAR ( serialSeq.NEXTVAL ) AS serial_no
         , 0 AS seqNo
         , 'SERIAL_DEVICES' AS test_alloc
    FROM DUAL
  CONNECT BY LEVEL <= p_volume
   ;
 COMMIT ;
END serial_device_inserts ;

PROCEDURE non_serial_device_inserts ( p_custOrder IN NUMBER , p_partNum IN VARCHAR2 , p_volume IN NUMBER )
IS
BEGIN
   INSERT INTO non_serial_devices ( pk_value , custOrder , part_no , seqNo , test_alloc )
   SELECT SYS_GUID() AS pk_value
        , p_custOrder AS custOrder
        , p_partNum AS part_no
        , 0 AS seqNo
        , 'NON_SERIAL_DEVICES' AS test_alloc
     FROM DUAL
   CONNECT BY LEVEL <= p_volume
   ;
   COMMIT ;
END non_serial_device_inserts ;

PROCEDURE get_device_detail ( v_testName IN VARCHAR2 , v_rec_out OUT SYS_REFCURSOR )
IS
   v_device hub3_devices%ROWTYPE ;
   v_deviceQ skyq_devices%ROWTYPE ;
   v_devicesl serial_devices%ROWTYPE ;
   v_deviceNs non_serial_devices%ROWTYPE ;
   v_deviceEst est_sales_device_info%ROWTYPE ;
BEGIN
   -- Select and lock first accountnumber for a given test.
   IF UPPER ( v_testName ) = 'HUB3_DEVICES'
   THEN
      SELECT t.* INTO v_device
        FROM hub3_devices t
       WHERE t.seqNo = ( SELECT MIN ( t2.seqNo ) FROM hub3_devices t2 )
         AND ROWNUM = 1
         FOR UPDATE
      ;
      UPDATE hub3_devices t
         SET t.seqNo = t.seqNo + 1
           , t.outputted = SYSDATE
       WHERE t.pk_value = v_device.pk_value
      ;
      COMMIT ;
      OPEN v_rec_out FOR
         SELECT t.*
           FROM hub3_devices t
          WHERE t.pk_value = v_device.pk_value
      ;
   ELSIF UPPER ( v_testName ) = 'HUB3_CONFIRM'
   THEN
      raise_application_error ( -20003 , 'HUB3_CONFIRM removed Jul-2022 by Andrew Fraser because appeared to not have been used since 2019.' ) ;
   ELSIF UPPER ( v_testName ) = 'SKYQ_DEVICES'
   THEN
      SELECT t.* INTO v_deviceQ
        FROM skyq_devices t
       WHERE t.test_alloc = UPPER ( v_testName )
         AND t.seqNo = ( SELECT MIN ( t2.seqNo ) FROM skyq_devices t2 WHERE t2.test_alloc = UPPER ( v_testName ) )
         AND ROWNUM = 1
         FOR UPDATE
      ;
      UPDATE skyq_devices t
         SET t.seqNo = t.seqNo + 1
           , t.outputted = SYSDATE
       WHERE t.pk_value = v_deviceQ.pk_value
      ;
      COMMIT ;
      OPEN v_rec_out FOR
         SELECT t.*
           FROM skyq_devices t
          WHERE t.pk_value = v_deviceQ.pk_value
      ;
   ELSIF UPPER ( v_testName ) = 'SERIAL_DEVICES'
   THEN
      SELECT t.* INTO v_devicesl
        FROM serial_devices t
       WHERE t.test_alloc = UPPER ( v_testName )
         AND t.seqNo = ( SELECT MIN ( t2.seqNo ) FROM serial_devices t2 WHERE t2.test_alloc = UPPER ( v_testName ) )
         AND ROWNUM = 1
         FOR UPDATE
      ;
      UPDATE serial_devices t
         SET t.seqNo = t.seqNo + 1
           , t.outputted = SYSDATE
       WHERE t.pk_value = v_devicesl.pk_value
      ;
      COMMIT ;
      OPEN v_rec_out FOR
         SELECT t.*
           FROM serial_devices t
          WHERE t.pk_value = v_devicesl.pk_value
      ;
   ELSIF UPPER ( v_testName ) = 'NON_SERIAL_DEVICES'
   THEN
      SELECT t.* INTO v_deviceNs
        FROM non_serial_devices t
       WHERE t.test_alloc = UPPER ( v_testName )
         AND t.seqNo = ( SELECT MIN ( t2.seqNo ) FROM non_serial_devices t2 WHERE t2.test_alloc = UPPER ( v_testName ) )
         AND ROWNUM = 1
         FOR UPDATE
      ;
      UPDATE non_serial_devices t
         SET t.seqNo = t.seqNo + 1
           , t.outputted = SYSDATE
       WHERE t.pk_value = v_deviceNs.pk_value
      ;
      COMMIT ;
      OPEN v_rec_out FOR
         SELECT t.*
           FROM non_serial_devices t
          WHERE t.pk_value = v_deviceNs.pk_value
      ;
   ELSIF UPPER ( v_testName ) = 'EST_SALES_DEVICE_INFO'
   THEN
      SELECT t.* INTO v_deviceEst
        FROM est_sales_device_info t
       WHERE t.test_alloc = UPPER ( v_testName )
         AND t.seqNo = ( SELECT MIN ( t2.seqNo ) FROM non_serial_devices t2 WHERE t2.test_alloc = UPPER ( v_testName ) )
         AND ROWNUM = 1
         FOR UPDATE
      ;
      UPDATE est_sales_device_info t
         SET t.seqNo = t.seqNo + 1
           , t.outputted = SYSDATE
       WHERE t.pk_value = v_deviceEst.pk_value
      ;
      COMMIT ;
      OPEN v_rec_out FOR
         SELECT t.*
           FROM est_sales_device_info t
          WHERE t.pk_value = v_deviceEst.pk_value
      ;
   END IF ;
EXCEPTION
   WHEN no_data_found
   THEN
      raise_application_error ( -20003 , 'Test supplied (' || UPPER ( v_testName ) || ') has no remaining data.' ) ;
END get_device_detail ;

END device_data_pkg ;
/
