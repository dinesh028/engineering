SET SERVEROUTPUT ON ;
SET AUTOCOMMIT OFF;
DECLARE
  delete_stmt_str VARCHAR2(10000);
  CURSOR c_child_tables
  IS
    select a.table_name parent_table_name, b.r_constraint_name parent_constraint, c.column_name parent_column, b.table_name child_table, b.constraint_name as child_constraint, d.column_name child_column from all_constraints a, 
all_constraints b, all_cons_columns c, all_cons_columns d
where a.owner='HUE' and a.table_name like 'AUTH_USER' and a.CONSTRAINT_TYPE in ('P', 'U') and a.constraint_name=b.r_constraint_name and a.owner=c.owner and a.table_name=c.table_name
and b.r_constraint_name=c.constraint_name and 
b.owner=d.owner and b.table_name=d.table_name
and b.constraint_name=d.constraint_name;

CURSOR users_to_delete
is
select id, username from HUE.AUTH_USER where username in ('}hF''LYds2h3mds;3K','abcd');


BEGIN

for delete_id in users_to_delete
Loop
dbms_output.put_line(delete_id.id || '--'|| delete_id.username);
delete_stmt_str := 'delete from HUE.DESKTOP_DOCUMENT_TAGS where document_id in (select id from HUE.DESKTOP_DOCUMENT where OWNER_ID='||delete_id.id ||')';
dbms_output.put_line(delete_stmt_str);
EXECUTE IMMEDIATE delete_stmt_str;
  FOR c_child IN c_child_tables
  LOOP
    dbms_output.put_line( c_child.parent_table_name|| '--' || c_child.parent_constraint ||'--'||c_child.parent_column || '--'||c_child.child_table ||'--'|| c_child.child_table ||'--'||c_child.child_column );
    delete_stmt_str := 'delete from HUE.' || c_child.child_table || ' where ' || c_child.child_column||'='||delete_id.id ;
    dbms_output.put_line(delete_stmt_str);
    EXECUTE IMMEDIATE delete_stmt_str;
    --ROLLBACK;
  END LOOP;
  delete_stmt_str := 'delete from HUE.AUTH_USER  where id='||delete_id.id ;
  dbms_output.put_line(delete_stmt_str);
  EXECUTE IMMEDIATE delete_stmt_str;
  commit;
  dbms_output.put_line('-----------------------------------------------------------');
End loop;
END;