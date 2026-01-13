defmodule MorgyDb.Repo do
  use Ecto.Repo,
    otp_app: :morgy_db,
    adapter: Ecto.Adapters.Postgres
end
