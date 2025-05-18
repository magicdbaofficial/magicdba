
create procedure maintenance.table_cloner
(
	@original_table_name	sysname
,	@clone_prefix			sysname			= 'clone'

,	@print_extended_logging	bit				= 0

,	@clone_table_fullname	sysname			= null	output

,	@is_raise_error_up		bit				= 1
,	@error_code				int				= 0		output
,	@error_message			nvarchar(255)	= null	output
)
as
/*
ver.1.25 Modified by magicdba team 2025-02-03
* add support of different lob data space filegroup
* add output paramter
* enhanced logging output
ver.1.22 Modified by magicdba team 2024-03-03
* add support of heap tables
* add partially support of computed columns
* some bugfixes
ver.1.20 Modified by magicdba team 2023-01-31
* disabled indexes support
* some bugfixes
ver.1.00 Created by magicdba team 2022-01-13
* first version of table cloner
*/
begin
	set transaction isolation level read committed
	set nocount on

	--служебные переменные
	declare	@sql_command				nvarchar(max)
		,	@original_table_schema		sysname
		,	@original_table_name_only	sysname
		,	@original_table_object_id	int
		,	@clone_table_name_only		sysname
		,	@clone_new_lob_fg			sysname
		,	@index_create_statement		nvarchar(max)
		,	@index_id					int
		,	@index_name					sysname
		,	@object_type				sysname
		,	@logging_delimeter			char(1)			= char(10)
		,	@is_outer_transaction		bit				= 0

	declare @index_definition			table(index_create_statement nvarchar(max) not null, index_id int not null, index_name sysname null, object_type sysname)

	set @error_code		= 0
	set @error_message	= null

	begin try
		if @@trancount > 0
			set @is_outer_transaction = 1
		else
			begin tran

		--[1] предварительные проверки
		if @clone_prefix is null or @original_table_name is null
			raiserror(N'Параметры @original_table_name и @clone_prefix должны быть заданы!', 16, 1);

		set @original_table_object_id = object_id(@original_table_name, 'U') 
		if @original_table_object_id is null
			raiserror(N'Оригинальная таблица %s не существует в текущей БД!', 16, 2, @original_table_name);

		set @original_table_schema		= object_schema_name(object_id(@original_table_name, 'U'))
		set @original_table_name_only	= object_name(object_id(@original_table_name, 'U'))

		set @clone_table_name_only		= @original_table_name_only + '_' + @clone_prefix
		set @clone_table_fullname		= quotename(@original_table_schema) + '.' + quotename(@clone_table_name_only)

		if object_id(@clone_table_fullname, 'U') is not null
			raiserror(N'Таблица-клон с именем %s уже существует в текущей БД!', 16, 3, @clone_table_fullname);

		--[2.1] сохраняем в табличную переменную информацию по индексам в оригинальной таблице
		insert	@index_definition
		select 
		--db_name() as database_name,
		--sc.name + N'.' + t.name as table_name,
		CASE si.index_id WHEN 0 
		THEN	N'ALTER TABLE ' + @clone_table_fullname + N' ADD temp_column_for_clustering int null;' + char(10)
				+ N'CREATE CLUSTERED INDEX temp_clustered_index ON ' + @clone_table_fullname + N'(temp_column_for_clustering)'
		ELSE 
			CASE is_primary_key WHEN 1 THEN
				N'ALTER TABLE ' + @clone_table_fullname + N' ADD CONSTRAINT ' + quotename(si.name + '_' + @clone_prefix) + N' PRIMARY KEY ' +
					CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '
				ELSE N'CREATE ' + 
					CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +
					CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +
					N'INDEX ' + quotename(si.name) + N' ON ' + @clone_table_fullname + N' '
			END +
			/* key def */ N'(' + key_definition + N')' +
			/* includes */ CASE WHEN include_definition IS NOT NULL THEN 
				N' INCLUDE (' + include_definition + N')'
				ELSE N''
			END +
			/* filters */ CASE WHEN filter_definition IS NOT NULL THEN 
				N' WHERE ' + filter_definition ELSE N''
			END
		END +
		/* with clause - compression goes here */
		CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL 
			THEN N' WITH (' +
				CASE WHEN row_compression_partition_list IS NOT NULL THEN
					N'DATA_COMPRESSION = ROW ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + row_compression_partition_list + N')' END
				ELSE N'' END +
				CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN N', ' ELSE N'' END +
				CASE WHEN page_compression_partition_list IS NOT NULL THEN
					N'DATA_COMPRESSION = PAGE ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + page_compression_partition_list + N')' END
				ELSE N'' END
			+ N')'
			ELSE N''
		END +
		/* ON where? filegroup? partition scheme? */
		' ON ' + CASE WHEN psc.name is null 
			THEN ISNULL(QUOTENAME(fg.name),N'')
			ELSE psc.name + N' (' + partitioning_column.column_name + N')' 
			END +
		N';' + 
		case when si.index_id = 0
			then char(10) + N'DROP INDEX temp_clustered_index on ' + @clone_table_fullname + ';'
				+ char(10) + N'ALTER TABLE ' + @clone_table_fullname + ' DROP COLUMN temp_column_for_clustering;'
			else
				case when si.is_disabled = 1
					then ' ALTER INDEX ' + quotename(si.name) + N' ON ' + @clone_table_fullname + N' DISABLE;'
					else '' 
				end
		end
		as index_create_statement,
		si.index_id,
		case is_primary_key when 1 then quotename(si.name + '_' + @clone_prefix) else quotename(si.name) end as index_name,
		--partition_sums.reserved_in_row_GB,
		--partition_sums.reserved_LOB_GB,
		--partition_sums.row_count,
		--user_updates as queries_that_modified,
		--partition_sums.partition_count,
		--si.allow_page_locks,
		--si.allow_row_locks,
		--si.is_hypothetical,
		--si.has_filter,
		--si.fill_factor,
		--si.is_unique,
		--isnull(pf.name, '/* Not partitioned */') as partition_function,
		--isnull(psc.name, fg.name) AS partition_scheme_or_filegroup,
		--t.create_date AS table_created_date,
		--t.modify_date AS table_modify_date
		case when o.object_id is null then 'INDEX' else 'OBJECT' end as object_type
		from		sys.indexes						as si
		join		sys.tables						as t	on si.object_id = t.object_id
		join		sys.schemas						as sc	on t.schema_id = sc.schema_id
		left join	sys.dm_db_index_usage_stats		as stat	on stat.database_id = db_id() and si.object_id = stat.object_id and si.index_id = stat.index_id
		left join	sys.partition_schemes			as psc	on si.data_space_id = psc.data_space_id
		left join	sys.partition_functions			as pf	on psc.function_id = pf.function_id
		left join	sys.filegroups					as fg	on si.data_space_id = fg.data_space_id
		left join	sys.filegroups					as lfg	on t.lob_data_space_id = lfg.data_space_id
		left join	sys.objects						as o	on o.name = si.name
		/* Key list */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + QUOTENAME(c.name) +
				CASE ic.is_descending_key WHEN 1 then N' DESC' ELSE N'' END
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON 
				ic.column_id=c.column_id  
				and ic.object_id=c.object_id
			WHERE ic.object_id = si.object_id
				and ic.index_id=si.index_id
				and ic.key_ordinal > 0
			ORDER BY ic.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS keys ( key_definition )
		/* Partitioning Ordinal */ OUTER APPLY (
			SELECT MAX(QUOTENAME(c.name)) AS column_name
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON 
				ic.column_id=c.column_id  
				and ic.object_id=c.object_id
			WHERE ic.object_id = si.object_id
				and ic.index_id=si.index_id
				and ic.partition_ordinal = 1) AS partitioning_column
		/* Include list */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + QUOTENAME(c.name)
			FROM sys.index_columns AS ic 
			JOIN sys.columns AS c ON 
				ic.column_id=c.column_id  
				and ic.object_id=c.object_id
			WHERE ic.object_id = si.object_id
				and ic.index_id=si.index_id
				and ic.is_included_column = 1
			ORDER BY c.name FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS includes ( include_definition )
		/* Partitions */ OUTER APPLY ( 
			SELECT 
				COUNT(*) AS partition_count,
				CAST(SUM(ps.in_row_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_in_row_GB,
				CAST(SUM(ps.lob_reserved_page_count)*8./1024./1024. AS NUMERIC(32,1)) AS reserved_LOB_GB,
				SUM(ps.row_count) AS row_count
			FROM sys.partitions AS p
			JOIN sys.dm_db_partition_stats AS ps ON
				p.partition_id=ps.partition_id
			WHERE p.object_id = si.object_id
				and p.index_id=si.index_id
			) AS partition_sums
		/* row compression list by partition */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
				and p.index_id=si.index_id
				and p.data_compression = 1
			ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS row_compression_clause ( row_compression_partition_list )
		/* data compression list by partition */ OUTER APPLY ( SELECT STUFF (
			(SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))
			FROM sys.partitions AS p
			WHERE p.object_id = si.object_id
				and p.index_id=si.index_id
				and p.data_compression = 2
			ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS page_compression_clause ( page_compression_partition_list )
		WHERE 
			si.type IN (0,1,2) /* heap, clustered, nonclustered */
			and si.object_id = object_id(@original_table_name)
		ORDER BY --table_name, 
				si.index_id
		option (recompile);

		--[2.2] создаем копию оригинальной таблицы с оригинальным именем
		raiserror(N'\> maintenance.table_cloner: создание пустой копии оригинальной таблицы %s под именем %s в заданной БД...%s', 10, 1, @original_table_name, @clone_table_fullname, @logging_delimeter) with nowait

		if not exists(select * from sys.computed_columns where object_id = object_id(@original_table_name))
		begin
			set @sql_command = 'select top(0) * into ' + @clone_table_fullname + ' from ' + @original_table_name
			if @print_extended_logging = 1
				print @sql_command + @logging_delimeter
			exec(@sql_command)
		end
		else
		begin
			set @sql_command = 
			'select top(0) ' + char(10)
			+
			stuff(
			(
			select		'	,	' + quotename(c.name) + char(10)
			from		sys.columns c
			where		c.object_id = object_id(@original_table_name) and c.is_computed = 0
			order by	c.column_id
			for xml path('')
			)
			, 2, 1, '')
			+ 'into	' + @clone_table_fullname + char(10)
			+ 'from	' + @original_table_name

			if @print_extended_logging = 1
				print @sql_command + @logging_delimeter
			exec(@sql_command)

			set @sql_command =
			(
			select		'alter table ' + @clone_table_fullname + ' add ' + quotename(c.name) + ' as ' + c.definition
						+ case when c.is_persisted = 1 then ' persisted' else '' end + ';' + char(10)
			from		sys.computed_columns c
			where		c.object_id = object_id(@original_table_name)
			order by	c.column_id
			for xml path('')
			)

			if @print_extended_logging = 1
				print @sql_command + @logging_delimeter
			exec(@sql_command)
		end

		--[2.3] перемещаем LOB файловую группу, если это необходимо
		select	@clone_new_lob_fg = max(case when object_id = object_id('test_table') then quotename(filegroup_name(lob_data_space_id)) else '' end)
		from	sys.tables
		where	lob_data_space_id != 0 and object_id in (@original_table_object_id, object_id(@clone_table_fullname))
		having	count(distinct lob_data_space_id) > 1

		if @clone_new_lob_fg is not null
		begin
			raiserror(N'\> maintenance.table_cloner: перемещение lob файловой группы таблицы-клона %s в %s%s', 10, 1, @clone_table_fullname, @clone_new_lob_fg, @logging_delimeter) with nowait

			set @sql_command = N'alter table ' + @clone_table_fullname + ' add temp_column_for_clustering int null;' + char(10) +
			N'create partition function pf_table_cloner_temp (int) as range right for values(0);' + char(10) +
			N'create partition scheme ps_table_cloner_temp as partition pf_table_cloner_temp to (' + @clone_new_lob_fg + ', ' + @clone_new_lob_fg + ');' + char(10) +
			N'create clustered index temp_clustered_index on ' + @clone_table_fullname + '(temp_column_for_clustering) on ps_table_cloner_temp(temp_column_for_clustering);' + char(10) +
			N'create clustered index temp_clustered_index on ' + @clone_table_fullname + '(temp_column_for_clustering) with(drop_existing=on)on ' + @clone_new_lob_fg + ';' + char(10) +
			N'drop index temp_clustered_index on ' + @clone_table_fullname + ';' + char(10) +
			N'alter table ' + @clone_table_fullname + ' drop column temp_column_for_clustering;' + char(10) +
			N'drop partition scheme ps_table_cloner_temp;' + char(10) +
			N'drop partition function pf_table_cloner_temp;'
			
			if @print_extended_logging = 1
				print @sql_command + @logging_delimeter
			exec(@sql_command)
		end

		--[2.4] цикл по всем индексам - создаем их на таблице-клоне
		declare index_list cursor local static read_only
			for
			select		index_create_statement
					,	index_id
					,	index_name
					,	object_type
			from		@index_definition
			order by	index_id

		open index_list
		
		fetch next from index_list into @index_create_statement, @index_id, @index_name, @object_type

		while @@fetch_status = 0
		begin
			if @index_id = 0
				raiserror(N'\> maintenance.table_cloner: перенос таблицы-клона в новую файловую группу...%s', 10, 1, @logging_delimeter) with nowait
			else
				raiserror(N'\> maintenance.table_cloner: создание объекта типа %s под именем %s в таблице-клоне...%s', 10, 1, @object_type, @index_name, @logging_delimeter) with nowait

			if @print_extended_logging = 1
				print @index_create_statement + @logging_delimeter
			exec(@index_create_statement)

			fetch next from index_list into @index_create_statement, @index_id, @index_name, @object_type
		end

		close index_list
		deallocate index_list

		--записываем ссылку на оригинальную таблицу в расширенные свойства таблицы клона
		exec sp_addextendedproperty 
			@name		= N'original_table_object_id'
		,	@value		= @original_table_object_id
		,	@level0type = N'schema'
		,	@level0name = @original_table_schema
		,	@level1type = N'table'
		,	@level1name = @clone_table_name_only

		if @is_outer_transaction = 0
			commit
	end try

	begin catch

		if @is_outer_transaction = 0
			if xact_state() != 0
				rollback

		exec maintenance.error_handler
			@is_raise_error_up	= @is_raise_error_up
		,	@error_message		= @error_message		output
		,	@error_number		= @error_code			output

	end catch
end
go