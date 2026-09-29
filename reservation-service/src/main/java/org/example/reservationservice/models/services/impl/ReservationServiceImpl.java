package org.example.reservationservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.reservationservice.models.constants.ReservationStatus;
import org.example.reservationservice.models.dto.requests.CreateReservationDetailRequest;
import org.example.reservationservice.models.dto.requests.CreateReservationRequest;
import org.example.reservationservice.models.dto.responses.ReservationDetailResponse;
import org.example.reservationservice.models.dto.responses.ReservationResponse;
import org.example.reservationservice.models.dto.responses.RoomResponse;
import org.example.reservationservice.models.entities.Reservation;
import org.example.reservationservice.models.entities.ReservationDetail;
import org.example.reservationservice.models.repositories.ReservationDetailRepository;
import org.example.reservationservice.models.repositories.ReservationRepository;
import org.example.reservationservice.models.services.ReservationService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ReservationServiceImpl implements ReservationService {

    private final ReservationRepository reservationRepository;
    private final ReservationDetailRepository reservationDetailRepository;
    private final RoomGatewayService roomGatewayService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    @Transactional
    public ReservationResponse createReservation(CreateReservationRequest request) {
        Reservation reservation = Reservation.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .totalAmount(0D)
                .status(ReservationStatus.PENDING)
                .build();

        reservation = reservationRepository.save(reservation);

        double totalAmount = 0D;
        List<ReservationDetailResponse> detailResponses = new ArrayList<>();

        for (CreateReservationDetailRequest item : request.items()) {
            RoomResponse room = roomGatewayService.getRoomById(item.roomId());
            double lineTotal = room.pricePerNight() * item.nights();
            totalAmount += lineTotal;

            ReservationDetail detail = ReservationDetail.builder()
                    .reservation(reservation)
                    .roomId(room.roomId())
                    .nights(item.nights())
                    .pricePerNight(room.pricePerNight())
                    .build();

            detail = reservationDetailRepository.save(detail);

            detailResponses.add(new ReservationDetailResponse(
                    detail.getId(),
                    detail.getRoomId(),
                    room.roomNumber(),
                    room.roomType(),
                    detail.getNights(),
                    detail.getPricePerNight(),
                    lineTotal
            ));
        }

        reservation.setTotalAmount(totalAmount);
        reservation = reservationRepository.save(reservation);

        kafkaTemplate.send("reservation-created", request.customerEmail());

        return new ReservationResponse(
                reservation.getId(),
                reservation.getCustomerName(),
                reservation.getCustomerEmail(),
                reservation.getTotalAmount(),
                reservation.getStatus(),
                detailResponses
        );
    }



}
