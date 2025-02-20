import pymssql
from sshtunnel import SSHTunnelForwarder




jiaoben = """CREATE PROC p_helpindex
    (
		@tbname sysname = '' ,
    @CLUSTERED INT = '1'
		)
AS 


    IF @tbname IS NULL
        OR @tbname = ''
        RETURN -1;



    DECLARE @t TABLE
        (
          table_name NVARCHAR(100) ,
          schema_name NVARCHAR(100) ,
          fill_factor INT ,
          is_padded INT ,
          ix_name NVARCHAR(100) ,
          type INT ,
          keyno INT ,
          column_name NVARCHAR(200) ,
          cluster VARCHAR(20) ,
          ignore_dupkey VARCHAR(20) ,
          [unique] VARCHAR(20) ,
          groupfile VARCHAR(10)
        );

    DECLARE @table_name NVARCHAR(100) ,
        @schema_name NVARCHAR(100) ,
        @fill_factor INT ,
        @is_padded INT ,
        @ix_name NVARCHAR(100) ,
        @ix_name_old NVARCHAR(100) ,
        @type INT ,
        @keyno INT ,
        @column_name NVARCHAR(100) ,
        @cluster VARCHAR(20) ,
        @ignore_dupkey VARCHAR(20) ,
        @unique VARCHAR(20) ,
        @groupfile VARCHAR(10);

    DECLARE ms_crs_ind CURSOR LOCAL STATIC
    FOR
        SELECT

DISTINCT        table_name = a.name ,
                schema_name = b.name ,
                fill_factor = c.OrigFillFactor ,
                is_padded = CASE WHEN c.status = 256 THEN 1
                                 ELSE 0
                            END ,
                ix_name = c.name ,
                type = c.indid ,
                d.keyno ,
                column_name = e.name
                + CASE WHEN INDEXKEY_PROPERTY(a.id, c.indid, d.keyno,
                                              'isdescending') = 1
                       THEN ' desc '
                       ELSE ''
                  END ,
                CASE WHEN ( c.status & 16 ) <> 0 THEN 'clustered'
                     ELSE 'nonclustered'
                END ,
                CASE WHEN ( c.status & 1 ) <> 0 THEN 'IGNORE_DUP_KEY'
                     ELSE ''
                END ,
                CASE WHEN ( c.status & 2 ) <> 0 THEN 'unique'
                     ELSE ''
                END ,
                g.groupname
        FROM    sysobjects a
                INNER JOIN sysusers b ON a.uid = b.uid
                INNER JOIN sysindexes c ON a.id = c.id
                INNER JOIN sysindexkeys d ON a.id = d.id
                                             AND c.indid = d.indid
                INNER JOIN syscolumns e ON a.id = e.id
                                           AND d.colid = e.colid
                INNER JOIN sysfilegroups g ON g.groupid = c.groupid
                LEFT JOIN master.dbo.spt_values f ON f.number = c.status
                                                     AND f.type = 'I'
        WHERE   a.id = OBJECT_ID(@tbname)
                AND c.indid < 255
                AND ( c.status & 64 ) = 0
                AND c.indid >= @CLUSTERED
        ORDER BY c.indid ,
                d.keyno;


    OPEN ms_crs_ind;

    FETCH ms_crs_ind INTO @table_name, @schema_name, @fill_factor, @is_padded,
        @ix_name, @type, @keyno, @column_name, @cluster, @ignore_dupkey,
        @unique, @groupfile;


    IF @@fetch_status < 0
        BEGIN

            DEALLOCATE ms_crs_ind;

            RAISERROR(15472,-1,-1); 

            RETURN -1;

        END;

    WHILE @@fetch_status >= 0
        BEGIN

            IF EXISTS ( SELECT  1
                        FROM    @t
                        WHERE   ix_name = @ix_name )
                UPDATE  @t
                SET     column_name = column_name + ',' + @column_name
                WHERE   ix_name = @ix_name;

            ELSE
                INSERT  INTO @t
                        SELECT  @table_name ,
                                @schema_name ,
                                @fill_factor ,
                                @is_padded ,
                                @ix_name ,
                                @type ,
                                @keyno ,
                                @column_name ,
                                @cluster ,
                                @ignore_dupkey ,
                                @unique ,
                                @groupfile;

            FETCH ms_crs_ind INTO @table_name, @schema_name, @fill_factor,
                @is_padded, @ix_name, @type, @keyno, @column_name, @cluster,
                @ignore_dupkey, @unique, @groupfile;


        END;

    DEALLOCATE ms_crs_ind;


    SELECT  'CREATE ' + UPPER([unique]) + CASE WHEN [unique] = '' THEN ''
                                               ELSE ' '
                                          END + UPPER(cluster) + ' INDEX '
            + ix_name + ' ON ' + table_name + '(' + column_name + ')'
            + CASE WHEN fill_factor > 0
                        OR is_padded = 1
                        OR ( UPPER(cluster) != 'NONCLUSTERED'
                             AND ignore_dupkey = 'IGNORE_DUP_KEY'
                           )
                   THEN ' WITH ' + CASE WHEN is_padded = 1 THEN 'PAD_INDEX,'
                                        ELSE ''
                                   END
                        + CASE WHEN fill_factor > 0
                               THEN 'FILLFACTOR =' + LTRIM(fill_factor)
                               ELSE ''
                          END
                        + CASE WHEN ignore_dupkey = 'IGNORE_DUP_KEY'
                                    AND UPPER(cluster) = 'NONCLUSTERED'
                               THEN CASE WHEN ( fill_factor > 0
                                                OR is_padded = 1
                                              ) THEN ',IGNORE_DUP_KEY'
                                         ELSE ',IGNORE_DUP_KEY'
                                    END
                               ELSE ''
                          END
                   ELSE ''
              END + ' ON [' + groupfile + ']' AS col,Index_name=ix_name,index_keys=column_name
    FROM    @t;
    RETURN 0;
"""
