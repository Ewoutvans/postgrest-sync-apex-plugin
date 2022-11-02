create or replace package body ep_postgrest_rest_plugin as

    c_date_format constant varchar2(20 char) := 'YYYY-MM-DD';
    c_limit constant number := 500;

    --==============================================================================
    type t_range is record (
        current_page_end number,
        current_fetched number,
        total_count number,
        has_more varchar2(10 char)
    );

    --==============================================================================
    -- This function will parse the postgrest content range header to tell how many
    -- we now fetched and if there are more.
    --==============================================================================
    function parse_range(p_content_range varchar2) return t_range is
        l_range t_range;

        l_split apex_t_varchar2;
        l_split_num apex_t_number;
    begin
        l_split := apex_string.split(p_content_range, '/');
        if l_split(2) = '*' then
            l_range.has_more := 'true';
        else
            l_range.total_count := number(l_split(2));
        end if;

        if l_split(1) = '*' then
            l_range.current_page_end := 0;
            l_range.current_fetched := 0;
            l_range.total_count := 0;
            l_range.has_more := 'false';
            return l_range;
        end if;

        l_split_num := apex_string.split_numbers(l_split(1), '-');
        l_range.current_page_end := l_split_num(2);
        l_range.current_fetched := (l_split_num(2) - l_split_num(1)) + 1;

        if l_range.current_page_end + 1 < l_range.total_count then
            l_range.has_more := 'true';
        else
            l_range.has_more := 'false';
        end if;
        return l_range;
    end;

    --==============================================================================
    -- this procedure builds a request body. We replace the build in function because
    -- that one doesn't seem to work? (tested with 22.1.6).
    --
    -- The following JSON is generated { "col1": "val2", "col2": "val2" }
    --==============================================================================
    function build_request_body(
        p_profile_id in number
        , p_profile_columns in apex_plugin.t_web_source_columns
        , p_values_context in apex_exec.t_context
    ) return clob is
        l_result clob;
        l_column apex_exec.t_column;
        l_data_type apex_exec.t_data_type;

        function get_column_selector(p_name in apex_appl_data_profile_cols.name%type) return apex_appl_data_profile_cols.column_selector%type is
            l_column_selector apex_appl_data_profile_cols.column_selector%type;
        begin
            select column_selector
            into l_column_selector
            from apex_appl_data_profile_cols
            where data_profile_id = p_profile_id
                and name = p_name;

            return l_column_selector;
        end;
    begin
        apex_json.initialize_clob_output( p_preserve => false );
        apex_json.open_object;
        begin
            if apex_exec.next_row(p_values_context) then
                for i in 1..apex_exec.get_column_count(p_values_context) loop
                    l_column := apex_exec.get_column(p_values_context, i);
                    l_data_type := l_column.data_type;

                    if l_data_type = apex_exec.c_data_type_number then
                        apex_json.write(get_column_selector(l_column.name), apex_exec.get_number(p_context => p_values_context, p_column_name => l_column.name));
                    elsif l_data_type = apex_exec.c_data_type_varchar2 then
                        apex_json.write(get_column_selector(l_column.name), apex_exec.get_varchar2(p_context => p_values_context, p_column_name => l_column.name));
                    elsif l_data_type = apex_exec.c_data_type_date then
                        apex_json.write(get_column_selector(l_column.name), to_char(apex_exec.get_date(p_context => p_values_context, p_column_name => l_column.name), c_date_format));
                    else
                        raise_application_error(20001, 'DataType not supported');
                    end if;
                end loop;
            end if;
        exception when others then
            raise;
        end;
        apex_json.close_object;

        l_result := apex_json.get_clob_output;
        apex_json.free_output;
        return l_result;
    end;

    --==============================================================================
    -- REST Source Capabilities Procedure
    --
    -- This procedure tells APEX whether the Plug-In (and the REST API) supports
    -- pagination (large result sets come as multiple pages), server-side filtering
    -- and server-side ordering.
    --
    -- The procedure implementation simply sets the "filtering", "pagination" or
    -- "order_by" members of the apex_plugin.t_web_source_capabilities record type
    -- to either true or false.
    --==============================================================================
    procedure capabilities_postgrest(
        p_plugin in apex_plugin.t_plugin
        , p_result in out nocopy apex_plugin.t_web_source_capabilities
    ) is
    begin
        p_result.filtering := true;
        p_result.pagination := true;
        p_result.order_by := true;
    end;

    --==============================================================================
    -- REST Source Discovery Procedure
    --
    -- This procedure is called by APEX during the REST Data Source creation, when
    -- the "Discover" button is clicked. This procedure can:
    -- * return structured information about the columns, data types and
    --   JSON or XML selectors
    -- * return a JSON example which APEX then will sample to derive columns and
    --   data types
    --==============================================================================
    procedure discover_postgrest(
        p_plugin in wwv_flow_plugin_api.t_plugin
        , p_web_source in wwv_flow_plugin_api.t_web_source
        , p_params in wwv_flow_plugin_api.t_web_source_discover_params
        , p_result in out nocopy wwv_flow_plugin_api.t_web_source_discover_result
    ) is
        l_web_source_operation apex_plugin.t_web_source_operation;
        l_dummy_parameters apex_plugin.t_web_source_parameters;
        l_time_budget number := 60;

        c_query_param_limit constant varchar2(10) := 'limit';
        c_query_limit_value constant varchar2(10) := '50';
    begin
        --
        -- Discovery is based on the "fetch rows" operation of a REST Data Source; GET operation.
        --

        --
        -- Prepare web source
        --
        l_web_source_operation := apex_plugin_util.get_web_source_operation(
            p_web_source => p_web_source
            , p_db_operation => apex_plugin.c_db_operation_fetch_rows
            , p_perform_init => true
        );

        --
        -- Adjust the query string attribute of the REST operation to use the computed query parameter
        --
        l_web_source_operation.query_string := c_query_param_limit || '=' || c_query_limit_value;

        --
        -- Perform the REST request. We'll receive the JSON response in the "p_result.sample_response" variable.
        --
        apex_web_service.g_request_headers(apex_web_service.g_request_headers.count+1).name := 'Prefer';
        apex_web_service.g_request_headers(apex_web_service.g_request_headers.count).value := 'count=exact';
        apex_plugin_util.make_rest_request(
            p_web_source_operation => l_web_source_operation
            , p_bypass_cache => false
            , p_time_budget => l_time_budget
            , p_response => p_result.sample_response
            , p_response_parameters => l_dummy_parameters
        );

        -- set the response headers received by the REST API for display in the Discovery Results screen
        p_result.response_headers := apex_web_service.g_headers;
    exception
        when others then
            apex_debug.error('Unhandled Exception');
        raise;
    end;

    --==============================================================================
    -- REST Source Fetch Procedure
    --
    -- This procedure does the actual "Fetch" operation when rows are being
    -- requested from the REST Data Source. When an APEX component is about to
    -- render, APEX computes the first row and the amount of rows required. This
    -- and all dynamic filter and order by information is passed to the
    -- procedure as the "p_params" parameter.
    --==============================================================================
    procedure fetch_postgrest(
        p_plugin in apex_plugin.t_plugin
        , p_web_source in apex_plugin.t_web_source
        , p_params in apex_plugin.t_web_source_fetch_params
        , p_result in out nocopy apex_plugin.t_web_source_fetch_result
    ) is
        l_web_source_operation apex_plugin.t_web_source_operation;
        l_time_budget number := 60;
        l_range t_range;
        l_offset number;
        l_first_offset pls_integer;
        l_page_to_fetch pls_integer := 0;
        l_continue_fetching boolean := true;
        l_query apex_t_varchar2 := apex_t_varchar2();
        l_order_by apex_t_varchar2 := apex_t_varchar2();
        l_order_by_columns apex_t_varchar2 := apex_t_varchar2();
        l_columns apex_t_varchar2 := apex_t_varchar2();
        l_filter_type_id pls_integer;
        l_filter_type varchar2(15 char);

        l_content_range varchar2(15 char);
        l_prefer_count boolean := false;
        l_search_column varchar2(50 char);
        l_col varchar2(100 char);
        l_val1 varchar2(255 char);
        l_val2 varchar2(255 char);
        l_filter varchar2(255 char);

        function get_column_selector(p_name in apex_appl_data_profile_cols.name%type) return apex_appl_data_profile_cols.column_selector%type is
            l_column_selector apex_appl_data_profile_cols.column_selector%type;
        begin
            -- TODO what if select into fails?
            select column_selector
            into l_column_selector
            from apex_appl_data_profile_cols
            where data_profile_id = p_web_source.profile_id
                and name = p_name;

            return l_column_selector;
        end;

        function get_data_type(p_name in apex_appl_data_profile_cols.name%type) return apex_appl_data_profile_cols.data_type%type is
            l_data_type apex_appl_data_profile_cols.data_type%type;
        begin
            select data_type
            into l_data_type
            from apex_appl_data_profile_cols
            where data_profile_id = p_web_source.profile_id
                and name = p_name;
            return l_data_type;
        end;
    begin
        --
        -- Prepare web source
        --
        l_web_source_operation := apex_plugin_util.get_web_source_operation(
            p_web_source => p_web_source
            , p_db_operation => apex_plugin.c_db_operation_fetch_rows
            , p_perform_init => true
        );

        --
        -- Initialize the response output. An invocation of the "Fetch" procedure can also return multiple
        -- JSON or XML documents, so responses are maintained as an instance of the APEX_T_CLOB (array of CLOB) type
        --
        p_result.responses := apex_t_clob();

        --
        -- Set needed headers
        --
        apex_web_service.g_request_headers.delete;
        for i in 1 .. l_web_source_operation.parameters.count loop
            if l_web_source_operation.parameters(i).param_type = apex_plugin.c_web_src_param_header then
                -- handle HTTP header parameters
                apex_web_service.g_request_headers(apex_web_service.g_request_headers.count + 1).name := l_web_source_operation.parameters(i).name;
                apex_web_service.g_request_headers(apex_web_service.g_request_headers.count).value := l_web_source_operation.parameters(i).value;
                -- Make sure the Prefer count header is set
                if upper(l_web_source_operation.parameters(i).name) = upper('Prefer') then
                    l_prefer_count := true;
                end if;
            end if;
        end loop;

        -- Make sure the Prefer count header is set
        if not l_prefer_count then
            apex_web_service.g_request_headers(apex_web_service.g_request_headers.count+1).name := 'Prefer';
            apex_web_service.g_request_headers(apex_web_service.g_request_headers.count).value := 'count=exact';
        end if;

        --
        -- Select only columns we need
        --
        if p_params.requested_columns.count <> 0 then
            for c in 1..p_params.requested_columns.count loop
                apex_string.push(l_columns, lower(p_params.requested_columns(c).name));
            end loop;
            apex_string.push(l_query, 'select='||apex_string.join(l_columns, ','));
        end if;


        --
        -- Handle the filters, not all are available in postgrest all with a - in front are not implemented
        -- TODO: handle external filters first & skip if asked again.
        --
        /* Filter types handled
        c_filter_eq              constant t_filter_type := 1;
        c_filter_not_eq          constant t_filter_type := 2;
        c_filter_gt              constant t_filter_type := 3;
        c_filter_gte             constant t_filter_type := 4;
        c_filter_lt              constant t_filter_type := 5;
        c_filter_lte             constant t_filter_type := 6;
        c_filter_null            constant t_filter_type := 7;
        c_filter_not_null        constant t_filter_type := 8;
        c_filter_starts_with     constant t_filter_type := 9;
        c_filter_not_starts_with constant t_filter_type := 10;
        c_filter_ends_with       constant t_filter_type := 11;
        c_filter_not_ends_with   constant t_filter_type := 12;
        c_filter_contains        constant t_filter_type := 13;
        c_filter_not_contains    constant t_filter_type := 14;
        c_filter_in              constant t_filter_type := 15;
        c_filter_not_in          constant t_filter_type := 16;
        c_filter_between         constant t_filter_type := 17;
        -c_filter_between_lbe     constant t_filter_type := 29;
        -c_filter_between_ube     constant t_filter_type := 30;
        -c_filter_not_between     constant t_filter_type := 18;
        c_filter_regexp          constant t_filter_type := 19;
        -c_filter_last            constant t_filter_type := 20;
        -c_filter_not_last        constant t_filter_type := 21;
        -c_filter_next            constant t_filter_type := 22;
        -c_filter_not_next        constant t_filter_type := 23;
        c_filter_like            constant t_filter_type := 24;
        c_filter_not_like        constant t_filter_type := 25;
        -c_filter_search          constant t_filter_type := 26;
        -c_filter_sql_expression  constant t_filter_type := 27;
        -c_filter_oracletext      constant t_filter_type := 28;
        -c_filter_sdo_filter      constant t_filter_type := 31;
        -c_filter_sdo_anyinteract constant t_filter_type := 32;
       */
        for f in 1..p_params.filters.count loop
            l_filter_type_id := p_params.filters(f).filter_type;
            l_val1 := coalesce(to_char(p_params.filters(f).filter_values(1).number_value), p_params.filters(f).filter_values(1).varchar2_value, to_char(p_params.filters(f).filter_values(1).date_value, c_date_format));
            if p_params.filters(f).filter_values.count >= 2 then
                l_val2 := coalesce(to_char(p_params.filters(f).filter_values(2).number_value), p_params.filters(f).filter_values(2).varchar2_value, to_char(p_params.filters(f).filter_values(2).date_value, c_date_format));
            end if;

            -- Filter on all requested columns (ex. LOV). Generate query on all requested columns
            if p_params.filters(f).column_name is null then
                -- Find First search column
                for s in 1..p_params.filters(f).search_columns.count loop
                    l_search_column := p_params.filters(f).search_columns(s).name;
                    exit;
                end loop;
                if l_search_column is not null then
                    -- postgrest does * instead of % so we don't accidentally do url encodings
                    null;
                    -- TODO: not yet clear what i should do here
                    --apex_string.push(l_query, get_column_selector(l_search_column) || '=ilike.*' || replace(l_val1, '%', '*') || '*');
                end if;
                continue;
            else
                l_search_column := p_params.filters(f).column_name;
                l_col := get_column_selector(l_search_column);
            end if;

            if l_filter_type_id in (apex_exec.c_filter_eq, apex_exec.c_filter_not_eq, apex_exec.c_filter_gt, apex_exec.c_filter_gte, apex_exec.c_filter_lt, apex_exec.c_filter_lte) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_eq then '=eq'
                    when apex_exec.c_filter_not_eq then '=not.eq'
                    when apex_exec.c_filter_gt then '=gt'
                    when apex_exec.c_filter_gte then '=gte'
                    when apex_exec.c_filter_lt then '=lt'
                    when apex_exec.c_filter_lte then '=lte'
                    end;
                l_filter := l_col || l_filter_type || '.' || l_val1;

            elsif l_filter_type_id in (apex_exec.c_filter_null, apex_exec.c_filter_not_null) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_null then '=is.null'
                    when apex_exec.c_filter_not_null then '=not.is.null'
                    end;
                l_filter := l_col || l_filter_type;

            elsif l_filter_type_id in (apex_exec.c_filter_starts_with, apex_exec.c_filter_not_starts_with) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_starts_with then '=ilike'
                    when apex_exec.c_filter_not_starts_with then '=not.ilike'
                    end;
                l_filter := l_col || l_filter_type || '.' || replace(l_val1, '%', '*') || '*';

            elsif l_filter_type_id in (apex_exec.c_filter_ends_with, apex_exec.c_filter_not_ends_with) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_ends_with then '=ilike'
                    when apex_exec.c_filter_not_ends_with then '=not.ilike'
                    end;
                l_filter := l_col || l_filter_type || '.*' || replace(l_val1, '%', '*');

            elsif l_filter_type_id in (apex_exec.c_filter_contains, apex_exec.c_filter_not_contains) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_contains then '=ilike'
                    when apex_exec.c_filter_not_contains then '=not.ilike'
                    end;
                l_filter := l_col || l_filter_type || '.*' || replace(l_val1, '%', '*') || '*';

            elsif l_filter_type_id in (apex_exec.c_filter_in, apex_exec.c_filter_not_in) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_in then '=in'
                    when apex_exec.c_filter_not_in then '=not.in'
                    end;
                l_filter := l_col || l_filter_type || '.(' || l_val1 || ')';

            elsif l_filter_type_id in (apex_exec.c_filter_between) then
                l_filter := 'and=(' || l_col || '.gte.'|| l_val1 ||',' || l_col || '.lte.'|| l_val2 ||')';

            elsif l_filter_type_id in (apex_exec.c_filter_regexp) then
                l_filter := l_col || '=imatch.' || l_val1;

            elsif l_filter_type_id in (apex_exec.c_filter_like, apex_exec.c_filter_not_like) then
                l_filter_type := case l_filter_type_id
                    when apex_exec.c_filter_like then '=ilike'
                    when apex_exec.c_filter_not_like then '=not.ilike'
                    end;
                l_filter := l_col || l_filter_type || '.' || replace(l_val1, '%', '*');

            else
                raise_application_error(20001, 'DataType not supported');
            end if;
            apex_string.push(l_query, l_filter);
        end loop;
        l_offset := (p_params.first_row - 1);
        l_first_offset := l_offset;

        --
        -- Probably a bug in apex. Offset is incorrectly passed if you do a search in a IR.
        -- when navigating pages in IR it works as intended.
        --
        if p_params.first_row < coalesce(p_params.max_rows, c_limit) then
            l_offset := l_offset * coalesce(p_params.max_rows, c_limit);
        end if;

        -- TODO: handle external order by, need to define a format and how to merge. No clue yet
        -- Handle order by
        for o in 1..p_params.order_bys.count loop
            apex_string.push(l_order_by
                , get_column_selector(p_params.order_bys(o).column_name)
                    || '.'
                    || case p_params.order_bys(o).direction when apex_exec.c_order_asc then 'asc' else 'desc' end
                    || '.'
                    || case p_params.order_bys(o).order_nulls when apex_exec.c_order_nulls_first then 'nullsfirst' else 'nullslast' end
            );
            apex_string.push(l_order_by_columns, p_params.order_bys(o).column_name);
        end loop;
        -- Handle external order by, if a normal order_by is already here. Use that one (user filters have priority)
        --apex_debug.error(p_params.external_order_bys);

        --
        -- If we are fetching all rows, fetch until the time budget is exhausted
        --
        while l_continue_fetching and coalesce(l_time_budget, 1) > 0 loop
            -- add a new member to the array of CLOB responses
            p_result.responses.extend(1);
            l_page_to_fetch := l_page_to_fetch + 1;

            --
            -- Build the query string by using the operation attribute and appending the page to fetch
            --
            if l_order_by.count > 0 then
                apex_string.push(l_query, 'order='||apex_string.join(l_order_by, ','));
            end if;
            l_web_source_operation.query_string := apex_string.join(l_query, '&');
            -- Add offset & limit
            if length(l_web_source_operation.query_string) > 0 then
                l_web_source_operation.query_string := l_web_source_operation.query_string || '&';
            end if;
            l_web_source_operation.query_string := l_web_source_operation.query_string || 'limit=' || coalesce(p_params.max_rows, c_limit) || '&offset=' || l_offset;

            --
            -- Perform the REST request. We'll receive the JSON response in the "p_result.sample_response" variable.
            --
            p_result.responses(l_page_to_fetch) := apex_web_service.make_rest_request(
                p_url => l_web_source_operation.url || '?' || l_web_source_operation.query_string
                , p_http_method => l_web_source_operation.http_method
                , p_credential_static_id => l_web_source_operation.credential_static_id
            );

            --
            -- Error handling
            -- Postgrest return 206: Partial Content if we still have extra rows
            --
            if apex_web_service.g_status_code not in (200, 201, 206) then
                apex_debug.error('HTTP error status:' || apex_web_service.g_status_code);
            end if;

            --
            -- Get header "Content-Range" to see if we need next fetch.
            --
            for i in 1..apex_web_service.g_headers.count loop
                if upper(apex_web_service.g_headers(i).name) = upper('Content-Range') then
                    l_content_range := apex_web_service.g_headers(i).value;
                end if;
            end loop;
            l_range := parse_range(l_content_range);
            l_offset := l_offset + l_range.current_fetched + 1;

            --
            -- If APEX requested "all rows" from the REST API and there are more rows to fetch,
            -- then continue fetching the next page
            --
            l_continue_fetching := p_params.fetch_all_rows and l_range.has_more = 'true';
        end loop;

        --
        -- Create the returning values
        --
        if p_params.fetch_all_rows then
            p_result.has_more_rows := false;
            p_result.response_row_count := l_offset;
            p_result.response_first_row := 1;
            p_result.total_row_count := l_range.total_count;
        else
            p_result.has_more_rows := (l_range.has_more = 'true');
            p_result.response_row_count := l_first_offset + l_range.current_fetched;
            p_result.response_first_row := l_first_offset + 1;
            p_result.total_row_count := l_range.total_count;
        end if;
    exception
        when others then
            apex_debug.error('Unhandled Exception');
        raise;
    end;

    --==============================================================================
    -- REST Source Fetch Procedure
    --
    -- This procedure does the actual "Fetch" operation when rows are being
    -- requested from the REST Data Source. When an APEX component is about to
    -- render, APEX computes the first row and the amount of rows required. This
    -- and all dynamic filter and order by information is passed to the
    -- procedure as the "p_params" parameter.
    --==============================================================================
    procedure dml_postgrest(
        p_plugin in apex_plugin.t_plugin
        , p_web_source in apex_plugin.t_web_source
        , p_params in apex_plugin.t_web_source_dml_params
        , p_result in out nocopy apex_plugin.t_web_source_dml_result
    ) is
        l_prefer_count boolean := false;
        l_web_source_operation apex_plugin.t_web_source_operation;
        l_request_body clob;
        l_response clob;
        l_return_values_insert apex_exec.t_context := p_params.insert_values_context;
        l_return_values_update apex_exec.t_context := p_params.update_values_context;
        l_return_values_delete apex_exec.t_context := p_params.delete_values_context;
        l_query apex_t_varchar2 := apex_t_varchar2();

        l_parsed_response json_array_t;
        l_row json_object_t;
    begin
        if apex_exec.get_total_row_count(p_params.insert_values_context) > 0 then
            --
            -- Prepare web source
            --
            l_web_source_operation := apex_plugin_util.get_web_source_operation(
                p_web_source => p_web_source
                , p_db_operation => apex_plugin.c_db_operation_insert
                , p_perform_init => true
            );

            --
            -- The inbuilt apex_plugin_util.build_request_body doesn't work?? use our own
            --
            l_request_body := build_request_body(
                p_profile_id => p_web_source.profile_id
                , p_profile_columns => p_web_source.profile_columns
                , p_values_context => p_params.insert_values_context
            );

            --
            -- Set needed headers
            --
            apex_web_service.g_request_headers.delete;
            for i in 1 .. l_web_source_operation.parameters.count loop
                if l_web_source_operation.parameters(i).param_type = apex_plugin.c_web_src_param_header then
                    -- handle HTTP header parameters
                    apex_web_service.g_request_headers(apex_web_service.g_request_headers.count + 1).name := l_web_source_operation.parameters(i).name;
                    apex_web_service.g_request_headers(apex_web_service.g_request_headers.count).value := l_web_source_operation.parameters(i).value;
                    -- Make sure the Prefer count header is set
                    if upper(l_web_source_operation.parameters(i).name) = upper('Prefer') then
                        l_prefer_count := true;
                    end if;
                end if;
            end loop;

            -- Make sure the Prefer count header is set
            if not l_prefer_count then
                apex_web_service.g_request_headers(apex_web_service.g_request_headers.count+1).name := 'Prefer';
                apex_web_service.g_request_headers(apex_web_service.g_request_headers.count).value := 'return=representation';
            end if;

            --
            -- Do the REST call
            --
            l_response := apex_web_service.make_rest_request(
                p_url => l_web_source_operation.url || '?' || l_web_source_operation.query_string
                , p_http_method => l_web_source_operation.http_method
                , p_credential_static_id => l_web_source_operation.credential_static_id
                , p_body => l_request_body
            );

            --
            -- Edit the context with the new information
            -- Currently only update Primary Key in returning values
            --
            l_parsed_response := json_array_t.parse(l_response);
            l_row := json_object_t(l_parsed_response.get(0));
            for sync_column in (select * from apex_appl_data_profile_cols where data_profile_id = p_web_source.profile_id and is_primary_key = 'Yes') loop
                apex_exec.set_value(
                    p_context => l_return_values_insert
                    , p_column_position => apex_exec.get_column_position(l_return_values_insert, sync_column.name)
                    , p_value => case when sync_column.data_type = 'NUMBER' then l_row.get_number(sync_column.column_selector) else l_row.get_string(sync_column.column_selector) end
                );
            end loop;
        elsif apex_exec.get_Total_row_count(p_params.update_values_context) > 0 then
            --
            -- Prepare web source
            --
            l_web_source_operation := apex_plugin_util.get_web_source_operation(
                p_web_source => p_web_source
                , p_db_operation => apex_plugin.c_db_operation_update
                , p_perform_init => true
            );

            --
            -- Get the request body
            --
            l_request_body := build_request_body(
                p_profile_id => p_web_source.profile_id
                , p_profile_columns => p_web_source.profile_columns
                , p_values_context => p_params.update_values_context
            );
            l_row := json_object_t.parse(l_request_body);

            --
            -- Calculate the Horizontal Filters so we don't update the whole table
            --
            for sync_column in (select * from apex_appl_data_profile_cols where data_profile_id = p_web_source.profile_id and is_primary_key = 'Yes') loop
                apex_string.push(l_query, sync_column.column_selector || '=eq.' || l_row.get_number(sync_column.column_selector));
            end loop;
            l_web_source_operation.query_string := apex_string.join(l_query, '&');

            --
            -- Do the REST call
            --
            l_response := apex_web_service.make_rest_request(
                p_url => l_web_source_operation.url || '?' || l_web_source_operation.query_string
                , p_http_method => l_web_source_operation.http_method
                , p_credential_static_id => l_web_source_operation.credential_static_id
                , p_body => l_request_body
            );
        elsif apex_exec.get_Total_row_count(p_params.delete_values_context) > 0 then
            --
            -- Prepare web source
            --
            l_web_source_operation := apex_plugin_util.get_web_source_operation(
                p_web_source => p_web_source
                , p_db_operation => apex_plugin.c_db_operation_delete
                , p_perform_init => true
            );

            --
            -- Get the request body
            --
            l_request_body := build_request_body(
                p_profile_id => p_web_source.profile_id
                , p_profile_columns => p_web_source.profile_columns
                , p_values_context => p_params.delete_values_context
            );
            l_row := json_object_t.parse(l_request_body);

            --
            -- Calculate the Horizontal Filters so we don't update the whole table
            --
            for sync_column in (select * from apex_appl_data_profile_cols where data_profile_id = p_web_source.profile_id and is_primary_key = 'Yes') loop
                apex_string.push(l_query, sync_column.column_selector || '=eq.' || l_row.get_number(sync_column.column_selector));
            end loop;
            l_web_source_operation.query_string := apex_string.join(l_query, '&');

            --
            -- Do the REST call
            --
            l_response := apex_web_service.make_rest_request(
                p_url => l_web_source_operation.url || '?' || l_web_source_operation.query_string
                , p_http_method => l_web_source_operation.http_method
                , p_credential_static_id => l_web_source_operation.credential_static_id
                , p_body => l_request_body
            );
        end if;

        --
        -- Return information
        --
        p_result.has_errors := false;
        p_result.insert_values_context := l_return_values_insert;
        p_result.update_values_context := l_return_values_update;
        p_result.delete_values_context := l_return_values_delete;
        p_result.has_errors := false;
    end;

end ep_postgrest_rest_plugin;

