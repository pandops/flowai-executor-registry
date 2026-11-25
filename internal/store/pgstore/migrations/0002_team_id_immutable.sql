-- An Executor and its connection record keep the team_id they were stored
-- with: no write, by the service or anyone else, may move them to another
-- team (service-platform, team-owned parent keeps its team).
CREATE FUNCTION team_id_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.team_id IS DISTINCT FROM OLD.team_id THEN
        RAISE EXCEPTION '%.team_id is immutable', TG_TABLE_NAME USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER executors_team_id_immutable BEFORE UPDATE OF team_id ON executors
    FOR EACH ROW EXECUTE FUNCTION team_id_immutable();

CREATE TRIGGER connections_team_id_immutable BEFORE UPDATE OF team_id ON connections
    FOR EACH ROW EXECUTE FUNCTION team_id_immutable();
