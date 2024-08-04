
task("test")
    on_run(function()
        import("core.base.task")
        task.run("unittest")
        task.run("perftest")
    end)
    set_menu {
        -- Settings menu usage
        usage = "xmake test [options]"
        , description = "run test case"
        , options = {
        }
    }

task("unittest")
    on_run(function()
        cprint('${green} 开始单元测试...')
        os.exec("tox -e py37")
    end)
    set_menu {
        -- Settings menu usage
        usage = "xmake unittest [options]"
        , description = "run test case"
        , options = {
        }
    }

task("perftest")
    on_run(function()
        cprint('开始性能测试...')
        import("core.base.task")
        task.run("perftest-websocket")
    end)
    set_menu {
        usage = "xmake perftest [options]"
        , description = "run test case"
        , options = {
        }
    }

task("perftest-websocket")
    on_run(function()
        math.randomseed(os.time())
        local loc = 'tests/perf/scripts'
        local message_load = 3000
        local host = "127.0.0.1"
        local port = math.random(1000, 8000)
        local websocket_server_script= 'src/websocket_server.go'
        local websocketd_cmd = string.format("sh -c 'timeout 30  go run %s --host %s --port %s &'", websocket_server_script, host, port)
        local websocket_server_addr = string.format("ws://%s:%s", host, port)
        local perftest_websocket_cmd = string.format("pyenv exec pytest --websocket-server=%s --websocket-message-load=%s tests/perf/websocket_test.py", websocket_server_addr, message_load)
        cprint('开始websocket采集性能测试...')
        os.cd(loc)
        os.exec(websocketd_cmd)
        os.cd('-')
        os.exec(perftest_websocket_cmd)
    end)

task("perftest-http")
    on_run(function()
        cprint('${green} 开始http采集性能测试...')
    end)

task("perftest-socket")
    on_run(function()
        cprint('${green} 开始socket采集性能测试...')
    end)




task("package")
    on_run(function()
        cprint('${green} 开始打包 ...')
        local package_cmd = "python3.7 setup.py sdist --formats=gztar,zip bdist_wheel"
        local clean_cmd1 = "python3.7 setup.py clean --all "
        local clean_cmd2 = "rm -rf dist"
        local clean_cmd3 =  "rm -rf build"
        os.exec(clean_cmd1)
        os.exec(clean_cmd2)
        os.exec(clean_cmd3)
        os.exec(package_cmd)
    end)


task("publish")
    on_run(function()
        import("core.base.task")
        task.run("package")
        local repo = ' https://github.com/WALL-EEEEEEE
        local publish_cmd = string.format("twine upload --repository-url %s dist/* ", repo)
        cprint('${green} 开始发布 ...')
        os.exec(publish_cmd)
    end)

    set_menu {
        -- Settings menu usage
        usage = "xmake pushlish [options]"
        , description = "publish package"
        , options = {
        }
    }