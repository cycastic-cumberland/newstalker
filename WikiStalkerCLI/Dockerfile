FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["WikiStalkerCLI/WikiStalkerCLI.csproj", "WikiStalkerCLI/"]
RUN dotnet restore "WikiStalkerCLI/WikiStalkerCLI.csproj"
COPY . .
WORKDIR "/src/WikiStalkerCLI"
RUN dotnet build "WikiStalkerCLI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "WikiStalkerCLI.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "WikiStalkerCLI.dll"]
