from yoyo import step
from yoyo import read_migrations
from yoyo import get_backend

backend = get_backend('postgres://postgres@localhost/migration_db')
migrations = read_migrations('C:\Users\user\Desktop\10Academy\week 12\Automation_dbt\migration_db')

with backend.lock():

    # Apply any outstanding migrations
    backend.apply_migrations(backend.to_apply(migrations))

    # Rollback all migrations
    backend.rollback_migrations(backend.to_rollback(migrations))