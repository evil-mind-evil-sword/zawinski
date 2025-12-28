const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library module
    const mod = b.addModule("zawinski", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });
    mod.addIncludePath(b.path("vendor/sqlite"));

    // SQLite static library
    const sqlite_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    sqlite_mod.addIncludePath(b.path("vendor/sqlite"));
    sqlite_mod.addCSourceFile(.{
        .file = b.path("vendor/sqlite/sqlite3.c"),
        .flags = &.{
            "-DSQLITE_ENABLE_FTS5",
            "-DSQLITE_ENABLE_JSON1",
            "-DSQLITE_OMIT_LOAD_EXTENSION",
        },
    });
    const sqlite = b.addLibrary(.{
        .name = "sqlite3",
        .root_module = sqlite_mod,
        .linkage = .static,
    });
    sqlite.linkLibC();

    // Executable
    const exe = b.addExecutable(.{
        .name = "jwz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zawinski", .module = mod },
            },
        }),
    });
    exe.root_module.addIncludePath(b.path("vendor/sqlite"));
    exe.linkLibrary(sqlite);
    b.installArtifact(exe);

    // Run step
    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // Test step
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    mod_tests.linkLibrary(sqlite);
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });
    exe_tests.linkLibrary(sqlite);
    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}
