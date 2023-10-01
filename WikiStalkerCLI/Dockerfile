FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["WikiStalker/WikiStalker.csproj", "WikiStalker/"]
RUN dotnet restore "WikiStalker/WikiStalker.csproj"
COPY . .
WORKDIR "/src/WikiStalker"
RUN dotnet build "WikiStalker.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "WikiStalker.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "WikiStalker.dll"]
