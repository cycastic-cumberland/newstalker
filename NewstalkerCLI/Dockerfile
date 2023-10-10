FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["NewstalkerCLI/NewstalkerCLI.csproj", "NewstalkerCLI/"]
RUN dotnet restore "NewstalkerCLI/NewstalkerCLI.csproj"
COPY . .
WORKDIR "/src/NewstalkerCLI"
RUN dotnet build "NewstalkerCLI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "NewstalkerCLI.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "NewstalkerCLI.dll"]
