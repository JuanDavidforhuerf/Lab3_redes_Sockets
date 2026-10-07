/*
 * subscriber_tcp.c
 * ---------------------------------------------------------------------
 * Subscriber (suscriptor) del sistema publicador-suscriptor, usando
 * sockets TCP. Se conecta al broker, se suscribe a un tema (partido),
 * y luego queda esperando en bucle los mensajes que el broker le
 * reenvíe, imprimiéndolos en pantalla en tiempo real.
 *
 * Protocolo de aplicación:
 *   Envía  -> SUB|<tema>|\n                 (una sola vez, al conectarse)
 *   Recibe -> MSG|<tema>|<contenido>\n       (cada vez que hay una publicación)
 *
 * Compilar:
 *   gcc subscriber_tcp.c -o subscriber_tcp
 *
 * Ejecutar:
 *   ./subscriber_tcp <IP_broker> <puerto_broker> <tema>
 *
 * Ejemplo:
 *   ./subscriber_tcp 127.0.0.1 5000 EquipoA_vs_EquipoB
 * ---------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>       // close()
#include <arpa/inet.h>      // inet_pton(), htons()
#include <sys/socket.h>     // socket(), connect(), send(), recv()
#include <netinet/in.h>     // struct sockaddr_in

#define TAM_BUFFER 1024

int main(int argc, char *argv[]) {

    if (argc != 4) {
        fprintf(stderr, "Uso: %s <IP_broker> <puerto_broker> <tema>\n", argv[0]);
        fprintf(stderr, "Ejemplo: %s 127.0.0.1 5000 EquipoA_vs_EquipoB\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *ip_broker = argv[1];
    int puerto_broker = atoi(argv[2]);
    const char *tema = argv[3];

    // socket(): crea el descriptor del socket.
    //   AF_INET     -> IPv4
    //   SOCK_STREAM -> TCP (orientado a conexión)
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        perror("Error al crear el socket");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in direccion_broker;
    direccion_broker.sin_family = AF_INET;
    direccion_broker.sin_port = htons(puerto_broker); // htons(): host-to-network short

    // inet_pton(): convierte la IP en formato texto a su representación
    // binaria dentro de la estructura sockaddr_in.
    if (inet_pton(AF_INET, ip_broker, &direccion_broker.sin_addr) <= 0) {
        fprintf(stderr, "Direccion IP invalida: %s\n", ip_broker);
        close(socket_fd);
        exit(EXIT_FAILURE);
    }

    // connect(): establece la conexión TCP con el broker (dispara el
    // three-way handshake SYN / SYN-ACK / ACK).
    if (connect(socket_fd, (struct sockaddr *)&direccion_broker, sizeof(direccion_broker)) < 0) {
        perror("Error al conectar con el broker");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }

    printf("[Subscriber] Conectado al broker %s:%d\n", ip_broker, puerto_broker);

    // Construir y enviar el mensaje de suscripción: SUB|tema|
    char mensaje_sub[TAM_BUFFER];
    snprintf(mensaje_sub, sizeof(mensaje_sub), "SUB|%s|\n", tema);

    // send(): envía la solicitud de suscripción al broker. A partir de
    // este momento, el broker registra este socket como interesado en
    // el tema indicado.
    if (send(socket_fd, mensaje_sub, strlen(mensaje_sub), 0) < 0) {
        perror("Error al enviar la suscripcion");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }

    printf("[Subscriber] Suscrito al tema: %s\n", tema);
    printf("[Subscriber] Esperando mensajes... (Ctrl+C para salir)\n\n");

    char buffer[TAM_BUFFER];

    // Bucle de recepción: el subscriber no envía más mensajes propios,
    // solo se queda esperando lo que el broker le reenvíe.
    while (1) {
        memset(buffer, 0, TAM_BUFFER);

        // recv(): bloquea el proceso hasta que llegue un mensaje nuevo
        // del broker, o hasta que la conexión se cierre (devuelve 0),
        // o haya un error (devuelve un valor negativo).
        int bytes_leidos = recv(socket_fd, buffer, TAM_BUFFER - 1, 0);

        if (bytes_leidos <= 0) {
            printf("\n[Subscriber] El broker cerro la conexion.\n");
            break;
        }

        // Quitar el salto de línea final si vino incluido.
        buffer[strcspn(buffer, "\n")] = '\0';

        // --- Parseo del mensaje recibido: MSG|tema|contenido ---
        char tipo[8] = {0};
        char tema_recibido[64] = {0};
        char contenido[TAM_BUFFER] = {0};

        // strtok() va modificando el buffer original, así que usamos
        // una copia para no perder el mensaje crudo si se necesitara.
        char copia[TAM_BUFFER];
        strncpy(copia, buffer, TAM_BUFFER - 1);
        copia[TAM_BUFFER - 1] = '\0';

        char *token = strtok(copia, "|");
        if (token != NULL) strncpy(tipo, token, sizeof(tipo) - 1);

        token = strtok(NULL, "|");
        if (token != NULL) strncpy(tema_recibido, token, sizeof(tema_recibido) - 1);

        token = strtok(NULL, "|");
        if (token != NULL) strncpy(contenido, token, sizeof(contenido) - 1);

        if (strcmp(tipo, "MSG") == 0) {
            printf(">> [%s] %s\n", tema_recibido, contenido);
        } else {
            // Por si llega algo con otro formato (no debería pasar en
            // condiciones normales, pero es buena práctica manejarlo).
            printf(">> (mensaje sin formato esperado): %s\n", buffer);
        }
    }

    // close(): cierra la conexión TCP de forma ordenada.
    close(socket_fd);

    return 0;
}