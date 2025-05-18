
create procedure maintenance.error_handler
(	@is_raise_error_up	bit				= 0
	
,	@error_message		nvarchar(2048)	= null	output
,	@error_severity		tinyint			= null	output
,	@error_state		tinyint			= null	output
,	@error_number		int				= null	output
,	@error_procedure	sysname			= null	output
,	@error_line			int				= null	output


)
as
/*
ver.1.0 Created by magicdba team 2019-03-15
Universal error handler
*/
begin
	set nocount on

	declare @is_user_error	bit	= case when error_number() = 50000 then 1 else 0 end

	select	@error_message		= error_message()
		,	@error_severity		= error_severity()
		,	@error_state		= error_state()
		,	@error_number		= case when @is_user_error = 1 then -error_state() else error_number() end
		,	@error_procedure	= error_procedure()
		,	@error_line			= error_line()


	if	@error_message not like '***%'
		  set @error_message =	'*** ' + case when @is_user_error = 1 then 'User error in ' else 'SQL Error in ' end
								+ coalesce(quotename(@error_procedure), '<dynamic SQL>')
								+ ', Line ' + ltrim(str(@error_line)) 
								+ '. Error ' + ltrim(str(@error_number)) 
								+ ': ' + @error_message

	if @is_raise_error_up = 1
		raiserror('%s', @error_severity, @error_state, @error_message)

	return
end
go
