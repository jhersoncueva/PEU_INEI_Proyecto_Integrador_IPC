% Leer el archivo de Excel correctamente
filename = 'data/data_indices.xlsx';

% Asegurarse de que 'data' sea una tabla al leerla
data = readtable(filename);

% Verifica las primeras filas de la tabla para asegurar que las columnas son correctas
disp(head(data));

% Convertir la columna FECHA a formato datetime si es necesario
data.FECHA = datetime(data.FECHA, 'InputFormat', 'yyyy-MM-dd');

% Extraer los datos por categorías (esto debe coincidir con las categorías en tus datos)
ipc_alimentos = data(strcmp(data.CATEGORIA, 'Índice Alimentos'), :);
ipc_salud = data(strcmp(data.CATEGORIA, 'Índice Salud'), :);
ipc_educacion = data(strcmp(data.CATEGORIA, 'Índice Educación'), :);
ipc_combustibles = data(strcmp(data.CATEGORIA, 'Índice Combustibles'), :);
ipc_vivienda = data(strcmp(data.CATEGORIA, 'Índice Vivienda'), :);
ipc_total = data(strcmp(data.CATEGORIA, 'IPC'), :);

% Crear una figura con 6 subgráficas
figure;

% Gráfico para el Índice Alimentos
subplot(3, 2, 1); 
plot(ipc_alimentos.FECHA, ipc_alimentos.VALOR, 'b', 'LineWidth', 2);
title('Índice Alimentos');
xlabel('Fecha');
ylabel('Índice (Base 100)');
grid on;

% Gráfico para el Índice Salud
subplot(3, 2, 2); 
plot(ipc_salud.FECHA, ipc_salud.VALOR, 'r', 'LineWidth', 2);
title('Índice Salud');
xlabel('Fecha');
ylabel('Índice (Base 100)');
grid on;

% Gráfico para el Índice Educación
subplot(3, 2, 3); 
plot(ipc_educacion.FECHA, ipc_educacion.VALOR, 'g', 'LineWidth', 2);
title('Índice Educación');
xlabel('Fecha');
ylabel('Índice (Base 100)');
grid on;

% Gráfico para el Índice Combustibles
subplot(3, 2, 4); 
plot(ipc_combustibles.FECHA, ipc_combustibles.VALOR, 'm', 'LineWidth', 2);
title('Índice Combustibles');
xlabel('Fecha');
ylabel('Índice (Base 100)');
grid on;

% Gráfico para el Índice Vivienda
subplot(3, 2, 5); 
plot(ipc_vivienda.FECHA, ipc_vivienda.VALOR, 'c', 'LineWidth', 2);
title('Índice Vivienda');
xlabel('Fecha');
ylabel('Índice (Base 100)');
grid on;

% Gráfico para el Índice Total
subplot(3, 2, 6); 
plot(ipc_total.FECHA, ipc_total.VALOR, 'k', 'LineWidth', 2);
title('Índice Total');
xlabel('Fecha');
ylabel('Índice (Base 100)');
grid on;


